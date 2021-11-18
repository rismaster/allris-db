package db

import (
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
	"github.com/rismaster/allris-common/application"
	"github.com/rismaster/allris-common/common/domtools"
	"github.com/rismaster/allris-common/common/files"
	"github.com/rismaster/allris-common/common/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Top struct {
	SILFDNR int
	TOLFDNR int
	VOLFDNR int

	SavedAt time.Time

	Betreff       string `datastore:",noindex"`
	Beschluss     string
	Protokoll     string `datastore:",noindex"`
	ProtokollRe   string `datastore:",noindex"`
	Nr            string
	Beschlussart  string
	Gremium       string
	Federfuehrend string
	Bearbeiter    string
	Datum         time.Time

	AbstimmungZustimmung int
	AbstimmungAblehnung  int
	AbstimmungEnthaltung int

	IndexTop        int
	Typ             string
	Status          string
	IndexBeratung   int
	BSVV            string
	Beschlussstatus string

	file    *files.File
	app     *application.AppContext
	anlagen []*Anlage
}

func NewTop(app *application.AppContext, file *files.File) (*Top, error) {

	var validID = regexp.MustCompile(`sitzung-([0-9]+)-top-([0-9]+)\.html`)
	matches := validID.FindStringSubmatch(file.GetName())
	if len(matches) >= 3 {

		silfdnr, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, err
		}
		tolfdnr, err := strconv.Atoi(matches[2])
		if err != nil {
			return nil, err
		}

		return &Top{
			SILFDNR: silfdnr,
			TOLFDNR: tolfdnr,
			file:    file,
			app:     app,
		}, nil
	}

	return nil, errors.New("filename pattern not match for " + file.GetName())

}

func (t *Top) GetDirectAnlagenQuery() *datastore.Query {
	return datastore.NewQuery(t.app.Config.GetEntityAnlage()).Ancestor(t.GetKey())
}

func (t *Top) GetSitzungKey() *datastore.Key {
	return datastore.NameKey(t.app.Config.GetEntitySitzung(), fmt.Sprintf("%d", t.SILFDNR), nil)
}

func (t *Top) GetKey() *datastore.Key {
	slog.Info(t.app.Config.GetEntityTop())
	slog.Info("%v", t.GetSitzungKey())
	slog.Info("%v", t.GetFile())
	slog.Info(strconv.Itoa(t.TOLFDNR))
	return datastore.NameKey(t.app.Config.GetEntityTop(), fmt.Sprintf("%d", t.TOLFDNR), t.GetSitzungKey())
}

func (t *Top) GetFile() *files.File {
	return t.file
}

func (t *Top) SetSavedAt(ti time.Time) {
	t.SavedAt = ti
}

func (t *Top) GetTops() []*Top {
	return []*Top{}
}

func (t *Top) GetAnlagen() []*Anlage {
	return t.anlagen
}

func (t *Top) GetTopQuery() *datastore.Query {
	return nil
}

func (t *Top) Parse(doc *goquery.Document) error {

	selector := "#allriscontainer"
	err := t.parseElement(doc.Find(selector).First())
	if err != nil {
		return err
	}
	return nil
}

func (t *Top) parseElement(dom *goquery.Selection) error {

	t.anlagen = ExtractAnlagen(dom, t.app.Config)

	for _, a := range t.anlagen {
		a.SILFDNR = t.SILFDNR
		a.TOLFDNR = t.TOLFDNR
	}

	t.Betreff = domtools.CleanText(strings.TrimPrefix(dom.Find("h1").Text(), "Auszug - "))

	allrisBS, _ := dom.Find("a[name=\"allrisBS\"]").
		NextFilteredUntil("div", "a").Html()
	t.Beschluss = domtools.SanatizeHtml(allrisBS, t.app.Config)

	allrisWP, _ := dom.Find("a[name=\"allrisWP\"]").
		NextFilteredUntil("div", "a").Html()
	t.Protokoll = domtools.SanatizeHtml(allrisWP, t.app.Config)

	allrisRE, _ := dom.Find("a[name=\"allrisRE\"]").
		NextFilteredUntil("div", "a").Html()
	t.ProtokollRe = domtools.SanatizeHtml(allrisRE, t.app.Config)

	t.parseAbstimmungsErgebnis(dom.Find("a[name=\"allrisAE\"]").
		NextFilteredUntil("div", "a"))

	bez, cont := domtools.ParseTable(dom.Find("table.tk1").Find("tr > td.kb1"))
	t.Nr = domtools.FindIndex(bez, cont, "TOP:")
	t.Beschlussart = domtools.FindIndex(bez, cont, "Beschlussart:")
	t.Status = domtools.FindIndexI(bez, cont, "Status:", 2)

	t.Gremium = domtools.FindIndex(bez, cont, "Gremium:")
	if t.Gremium == "" {
		t.Gremium = domtools.FindIndex(bez, cont, "Gremien:")
	}
	t.Federfuehrend = domtools.FindIndex(bez, cont, "Federführend:")
	t.Bearbeiter = domtools.FindIndex(bez, cont, "Bearbeiter/-in:")
	t.VOLFDNR = domtools.ExtractIntFromInput(dom, "VOLFDNR")

	datumString := domtools.FindIndex(bez, cont, "Datum:")
	datum, err2 := domtools.ExtractWeekdayDateFromCommaSeparated(datumString, "00:00", t.app.Config)
	if err2 != nil {
		return err2
	} else {
		t.Datum = datum
	}

	return nil
}

func (t *Top) parseAbstimmungsErgebnis(sel *goquery.Selection) {
	bez, cont := domtools.ParseTable(sel.Find("table tr td:first-child"))

	if bez == nil && cont == nil {
		sel.Find("p").Each(func(i int, selection *goquery.Selection) {
			descr := selection.Find("span").First().Text()
			if descr == "Zustimmung:" {
				t.AbstimmungZustimmung = domtools.StringToIntOrNeg(domtools.CleanText(selection.Find("span").Last().Text()))
			} else if descr == "Ablehnung:" {
				t.AbstimmungAblehnung = domtools.StringToIntOrNeg(domtools.CleanText(selection.Find("span").Last().Text()))
			} else if descr == "Enthaltung:" {
				t.AbstimmungEnthaltung = domtools.StringToIntOrNeg(domtools.CleanText(selection.Find("span").Last().Text()))
			}
		})
	} else {

		t.AbstimmungZustimmung = domtools.StringToIntOrNeg(domtools.FindIndex(bez, cont, "Zustimmung:"))
		t.AbstimmungAblehnung = domtools.StringToIntOrNeg(domtools.FindIndex(bez, cont, "Ablehnung:"))
		t.AbstimmungEnthaltung = domtools.StringToIntOrNeg(domtools.FindIndex(bez, cont, "Enthaltung:"))
	}
}

func (t *Top) UpdateAnlage(oldAnlage *Anlage, newAnlage *Anlage) *Anlage {
	oldAnlage.Title = newAnlage.Title
	return oldAnlage
}

func (t *Top) UpdateTop(oldTop *Top, newTop *Top) *Top {
	return oldTop
}
func (t *Top) SaveOrUpdate() error {
	tx, err := t.app.Db().NewTransaction(t.app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	var oldTop Top
	err = tx.Get(t.GetKey(), &oldTop)
	if err != nil && err != datastore.ErrNoSuchEntity {
		return err
	} else if err == nil {
		//update
		//uebernehmen von uebersicht

		t.BSVV = oldTop.BSVV
		t.Typ = oldTop.Typ
		t.IndexTop = oldTop.IndexTop
		t.IndexBeratung = oldTop.IndexBeratung
		t.Beschlussstatus = oldTop.Beschlussstatus
		t.SavedAt = time.Now()
	}

	_, err = tx.Put(t.GetKey(), t)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error saving to db top from %s", t.file.GetName()))
	}
	_, err = tx.Commit()
	return err
}

func (t *Top) Delete() error {

	ks, err := t.app.Db().GetAll(t.app.Ctx(), datastore.NewQuery(t.app.Config.GetEntityAnlage()).Ancestor(t.GetKey()).KeysOnly(), nil)
	if err != nil {
		return errors.Wrap(err, "error getting anlagen from db")
	}

	tx, err := t.app.Db().NewTransaction(t.app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	err = tx.DeleteMulti(ks)
	if err != nil {
		slog.Error("error delete anlagen of sitzung in db for %t: %v", t.file.GetName(), err)
	}

	err = tx.Delete(t.GetKey())
	if err != nil {
		slog.Error("error delete sitzung in db for %t: %v", t.file.GetName(), err)
	}

	_, err = tx.Commit()

	return err
}
