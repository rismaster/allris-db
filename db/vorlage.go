package db

import (
	"github.com/rismaster/allris-db/application"
	"github.com/rismaster/allris-common/common/domtools"
	"github.com/rismaster/allris-common/common/files"
	"github.com/rismaster/allris-common/common/slog"
	"github.com/rismaster/allris-db/config"
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Vorlage struct {
	VOLFDNR               int
	BSVV                  string
	Betreff               string `datastore:",noindex"`
	Status                string
	Federfuehrend         string
	Bearbeiter            string
	BeschlussVorlage      string `datastore:",noindex"`
	Begruendung           string `datastore:",noindex"`
	FinanzielleAuswirkung string `datastore:",noindex"`
	DatumAngelegt         time.Time
	BezueglichVOLFDNR     int
	BezueglichBSVV        string
	Bezueglich            *Vorlage

	beratungsfolge []*Top
	anlagen        []*Anlage

	SavedAt time.Time

	file *files.File
	app  *application.AppContext
}

func NewVorlage(app *application.AppContext, file *files.File) (*Vorlage, error) {

	volfdnrStr := strings.TrimPrefix(strings.TrimSuffix(file.GetName(), ".html"), "vorlage-")
	volfdnr, err := strconv.Atoi(volfdnrStr)
	if err != nil {
		return nil, err
	}
	return &Vorlage{
		VOLFDNR: volfdnr,
		file:    file,
		app:     app,
	}, nil
}

func (v *Vorlage) GetTopQuery() *datastore.Query {
	return datastore.NewQuery(config.EntityTop).Filter("VOLFDNR =", v.VOLFDNR)
}

func (v *Vorlage) GetDirectAnlagenQuery() *datastore.Query {
	return datastore.NewQuery(config.EntityAnlage).Ancestor(v.GetKey())
}

func (v *Vorlage) GetFile() *files.File {
	return v.file
}

func (v *Vorlage) SetSavedAt(t time.Time) {
	v.SavedAt = t
}

func (v *Vorlage) GetTops() []*Top {
	return v.beratungsfolge
}

func (v *Vorlage) GetAnlagen() []*Anlage {
	return v.anlagen
}

func (v *Vorlage) GetKey() *datastore.Key {
	return datastore.NameKey(config.EntityVorlage, fmt.Sprintf("%d", v.VOLFDNR), nil)
}

func (v *Vorlage) Parse(doc *goquery.Document) error {

	selector := "#allriscontainer"
	err := v.parseElement(doc.Find(selector).First())
	if err != nil {
		return err
	}
	return nil
}

func (v *Vorlage) parseElement(dom *goquery.Selection) error {

	topTblx := dom.Find("table.tk1")

	bez, cont := domtools.ParseTable(topTblx.Find("tr > td.kb1"))
	v.anlagen = ExtractAnlagen(dom)

	for _, a := range v.anlagen {
		a.VOLFDNR = v.VOLFDNR
	}

	basisanlagen := ExtractBasisAnlagen(dom)
	for _, a := range basisanlagen {
		v.anlagen = append(v.anlagen, a)
		a.VOLFDNR = v.VOLFDNR
	}
	theAnlagenTables := dom.Find("table.tk1")

	v.BSVV = domtools.CleanText(strings.TrimPrefix(dom.Find("h1").First().Text(), "Vorlage - "))

	v.BezueglichBSVV = domtools.FindIndex(bez, cont, "Bezüglich:")
	v.BezueglichVOLFDNR = domtools.ExtractIntFromInput(theAnlagenTables.Find("tr > td.ko1"), "VOLFDNR")
	v.Betreff = domtools.FindIndex(bez, cont, "Betreff:")
	v.Status = domtools.FindIndex(bez, cont, "Status:")
	v.Federfuehrend = domtools.FindIndex(bez, cont, "Federführend:")
	v.Bearbeiter = domtools.FindIndex(bez, cont, "Bearbeiter/-in:")

	bvhtml, _ := dom.Find("a[name=\"allrisBV\"]").NextFilteredUntil("div", "a").Html()
	v.BeschlussVorlage = domtools.SanatizeHtml(bvhtml)
	bghtml, _ := dom.Find("a[name=\"allrisSV\"]").
		NextFilteredUntil("div", "a").Html()
	v.Begruendung = domtools.SanatizeHtml(bghtml)
	fahtml, _ := dom.Find("a[name=\"allrisFA\"]").
		NextFilteredUntil("div", "a").Html()
	v.FinanzielleAuswirkung = domtools.SanatizeHtml(fahtml)

	//theTopTable := dom.Find(".me1 > table.tk1").First()

	//v.basisAnlagen = extractBasisAnlagen(theTopTable, v)

	topRows := dom.Find("table.tk1 table").Find("tr.zl12, tr.zl11")

	var err error
	var beratung *Top
	var missingBerDetails = true
	topRows.Each(func(i int, selection *goquery.Selection) {

		topTds := selection.Find("td")

		if topTds.Size() == 3 || topTds.Size() == 2 {
			if beratung != nil && missingBerDetails {
				v.beratungsfolge = append(v.beratungsfolge, beratung)
			}
			beratung = v.createBeratung(nil)
			beratung.Typ = domtools.CleanText(topTds.Next().Next().First().Text())
			beratung.Gremium = domtools.CleanText(topTds.Next().First().Text())
			status, exist := topTds.First().Attr("title")
			if exist {
				beratung.Beschlussstatus = domtools.CleanText(status)
			}
			missingBerDetails = true
			/*
				status, exist := topTds.First().Attr("title")
				if exist && beratung.Beschlussstatus == "" {
					beratung.Beschlussstatus = cleanText(status)
				}
			*/

		} else if topTds.Size() == 7 {

			missingBerDetails = false
			t, err := time.Parse(config.DateFormat, domtools.CleanText(topTds.Find("a").First().Text()))
			if err == nil {
				beratung.Datum = t
			}
			beratung.Beschlussart = domtools.CleanText(topTds.Next().Next().Next().Next().First().Text())
			beratung.SILFDNR = domtools.ExtractIntFromInput(topTds, "SILFDNR")
			beratung.TOLFDNR = domtools.ExtractIntFromInput(topTds, "TOLFDNR")
			status, exist := topTds.First().Attr("title")
			if exist {
				beratung.Beschlussstatus = domtools.CleanText(status)
			}
			if beratung.TOLFDNR <= 0 {
				topUrlStr, found := topTds.Next().Next().Find("form").Attr("action")
				if found {
					topUrl, err := url.Parse(topUrlStr)
					if err == nil {
						topSelected := topUrl.Query().Get("topSelected")
						beratung.TOLFDNR, err = strconv.Atoi(topSelected)
					}
				}

			}
			v.beratungsfolge = append(v.beratungsfolge, beratung)
			beratung = v.createBeratung(beratung)
		} else {
			err = errors.New(fmt.Sprintf("error parsing Beratungsfolge in VOLFDNR %d", v.VOLFDNR))
		}
	})
	if err != nil {
		return err
	}

	if beratung != nil && missingBerDetails {
		v.beratungsfolge = append(v.beratungsfolge, beratung)
	}

	var nulYear time.Time
	var filteredBeratungen []*Top
	for _, ber := range v.beratungsfolge {
		if ber.Datum != nulYear {
			filteredBeratungen = append(filteredBeratungen, ber)
		}

	}

	v.beratungsfolge = filteredBeratungen

	for index, b := range v.beratungsfolge {
		b.IndexBeratung = index
	}
	if v.Betreff == "" {
		return errors.New("leeres Betreff in Vorlage")
	}

	sort.SliceStable(v.beratungsfolge, func(i, j int) bool {
		return v.beratungsfolge[i].Datum.Before(v.beratungsfolge[j].Datum)
	})

	return nil
}

func (v *Vorlage) createBeratung(beratung *Top) *Top {
	beratungTyp := ""
	beratungGremium := ""
	if beratung != nil {
		beratungTyp = beratung.Typ
		beratungGremium = beratung.Gremium
	}
	beratung = new(Top)
	beratung.BSVV = v.BSVV
	beratung.Federfuehrend = v.Federfuehrend
	beratung.Status = v.Status
	beratung.VOLFDNR = v.VOLFDNR
	beratung.Typ = beratungTyp
	beratung.Gremium = beratungGremium
	beratung.SavedAt = time.Now()
	return beratung
}

func (v *Vorlage) UpdateTop(oldTop *Top, newTop *Top) *Top {

	if strings.TrimSpace(newTop.BSVV) != "" {
		oldTop.BSVV = newTop.BSVV
	}
	if strings.TrimSpace(newTop.Status) != "" {
		oldTop.Status = newTop.Status
	}
	if strings.TrimSpace(newTop.Typ) != "" {
		oldTop.Typ = newTop.Typ
	}
	oldTop.Beschlussstatus = newTop.Beschlussstatus
	oldTop.IndexBeratung = newTop.IndexBeratung
	oldTop.SavedAt = time.Now()

	return oldTop
}

func (v *Vorlage) UpdateAnlage(oldAnlage *Anlage, newAnlage *Anlage) *Anlage {
	oldAnlage.Title = newAnlage.Title
	return oldAnlage
}

func (v *Vorlage) SaveOrUpdate() error {
	tx, err := v.app.Db().NewTransaction(v.app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	var oldVorlage Vorlage
	err = tx.Get(v.GetKey(), &oldVorlage)
	if err != nil && err != datastore.ErrNoSuchEntity {
		return err
	} else if err == nil {
		//update
		//v.VOLFDNR = oldVorlage.VOLFDNR
	}

	_, err = tx.Put(v.GetKey(), v)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error saving to db vorlage from %s", v.file.GetName()))
	}

	_, err = tx.Commit()
	return err
}

func (v *Vorlage) Delete() error {

	ks, err := v.app.Db().GetAll(v.app.Ctx(), v.GetDirectAnlagenQuery().KeysOnly(), nil)
	if err != nil {
		return errors.Wrap(err, "error getting anlagen from db")
	}

	var tops []*Top
	_, err = v.app.Db().GetAll(v.app.Ctx(), v.GetTopQuery(), &tops)
	if err != nil {
		return errors.Wrap(err, "error getting beratungen from db")
	}

	tx, err := v.app.Db().NewTransaction(v.app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	for _, top := range tops {
		top.VOLFDNR = 0
		_, err1 := tx.Put(top.GetKey(), top)
		if err1 != nil {
			return errors.Wrap(err, fmt.Sprintf("error edit top volfdnr in db for %s", v.file.GetName()))
		}
	}

	err = tx.DeleteMulti(ks)
	if err != nil {
		slog.Error("error delete tops of vorlage in db for %s: %v", v.file.GetName(), err)
	}

	err = tx.Delete(v.GetKey())
	if err != nil {
		slog.Error("error delete vorlage in db for %s: %v", v.file.GetName(), err)
	}

	_, err = tx.Commit()

	return err
}
