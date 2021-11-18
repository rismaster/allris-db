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
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Sitzung struct {
	SILFDNR int
	Datum   time.Time
	Gremium string
	Status  string

	Title   string
	Uhrzeit string
	Raum    string
	Ort     string

	tops    []*Top
	anlagen []*Anlage

	SavedAt time.Time
	file    *files.File
	app     *application.AppContext
}

func NewSitzung(app *application.AppContext, file *files.File) (*Sitzung, error) {

	silfdnrStr := strings.TrimPrefix(strings.TrimSuffix(file.GetName(), ".html"), "sitzung-")
	silfdnr, err := strconv.Atoi(silfdnrStr)
	if err != nil {
		return nil, err
	}
	return &Sitzung{
		SILFDNR: silfdnr,
		file:    file,
		app:     app,
	}, nil
}

func (s *Sitzung) GetTopQuery() *datastore.Query {
	return datastore.NewQuery(s.app.Config.GetEntityTop()).Ancestor(s.GetKey())
}

func (s *Sitzung) GetDirectAnlagenQuery() *datastore.Query {
	return datastore.NewQuery(s.app.Config.GetEntityAnlage()).Ancestor(s.GetKey()).Filter("TOLFDNR = ", 0)
}

func (s *Sitzung) GetFile() *files.File {
	return s.file
}

func (s *Sitzung) SetSavedAt(t time.Time) {
	s.SavedAt = t
}

func (s *Sitzung) GetTops() []*Top {
	return s.tops
}

func (s *Sitzung) GetAnlagen() []*Anlage {
	return s.anlagen
}

func (s *Sitzung) GetKey() *datastore.Key {
	return datastore.NameKey(s.app.Config.GetEntitySitzung(), fmt.Sprintf("%d", s.SILFDNR), nil)
}

func (s *Sitzung) Parse(doc *goquery.Document) error {

	selector := "#allriscontainer"
	err := s.parseElement(doc.Find(selector).First())
	if err != nil {
		return err
	}
	return nil
}

func (s *Sitzung) parseElement(dom *goquery.Selection) error {

	bez, cont := domtools.ParseTable(dom.Find("table.tk1").Find("tr > td.kb1"))

	s.anlagen = ExtractAnlagen(dom, s.app.Config)

	for _, a := range s.anlagen {
		a.SILFDNR = s.SILFDNR
	}

	basisanlagen := ExtractBasisAnlagen(dom, s.app.Config)
	for _, a := range basisanlagen {
		s.anlagen = append(s.anlagen, a)
		a.SILFDNR = s.SILFDNR
	}

	s.Gremium = domtools.FindIndex(bez, cont, "Gremium:")
	s.Raum = domtools.FindIndex(bez, cont, "Raum:")
	s.Ort = domtools.FindIndex(bez, cont, "Ort:")
	s.Status = domtools.FindIndex(bez, cont, "Status:")

	s.Title = domtools.FindIndex(bez, cont, "Bezeichnung:")
	s.Uhrzeit = domtools.FindIndex(bez, cont, "Zeit:")
	datumString := domtools.FindIndex(bez, cont, "Datum:")

	datum, err := domtools.ExtractWeekdayDateFromCommaSeparated(datumString, s.Uhrzeit, s.app.Config)
	if err != nil {
		return err
	}
	s.Datum = datum

	topRows := dom.Find("table.tl1").Find("tr.zl12, tr.zl11")
	topRows.Each(func(i int, selection *goquery.Selection) {
		top := s.parseTop(selection)
		if top != nil {
			top.IndexTop = i
			s.tops = append(s.tops, top)
		}
	})

	return nil
}

func (s *Sitzung) parseTop(selection *goquery.Selection) *Top {

	topTds := selection.Find("td")
	top := &Top{
		SILFDNR: s.SILFDNR,
		TOLFDNR: domtools.ExtractIntFromInput(topTds, "TOLFDNR"),
		VOLFDNR: domtools.ExtractIntFromInput(topTds, "VOLFDNR"),
		Nr:      domtools.GetChildTextFromNode(topTds.Get(0)),
		Betreff: domtools.GetChildTextFromNode(topTds.Get(3)),
		BSVV:    domtools.GetChildTextFromNode(topTds.Get(5)),
		Datum:   s.Datum,
		Gremium: s.Gremium,
		SavedAt: time.Now(),
		app:     s.app,
	}

	topHref, exist := topTds.Find("a[title=\"Auswählen\"]").Attr("href")
	if exist {
		topHrefSplitted := strings.Split(topHref, "#")
		topHref = topHrefSplitted[0]
		m, err := url.ParseQuery(topHref)
		if err == nil {
			if val, ok := m["TOLFDNR"]; ok && len(val) > 0 {
				top.TOLFDNR = domtools.StringToIntOrNeg(val[0])
			}
		}
	}

	beschlussArt, exist := topTds.Find("input[type=\"submit\"][value=\"NA\"]").Attr("title")
	if exist {
		top.Beschlussart = beschlussArt
		if top.Beschlussart == "Auszug" {
			top.Beschlussart = ""
		}
	}
	return top
}

func (s *Sitzung) UpdateTop(oldTop *Top, newTop *Top) *Top {

	if strings.TrimSpace(newTop.BSVV) != "" {
		oldTop.BSVV = newTop.BSVV
	}
	if strings.TrimSpace(newTop.Status) != "" {
		oldTop.Status = newTop.Status
	}
	if strings.TrimSpace(newTop.Typ) != "" {
		oldTop.Typ = newTop.Typ
	}
	if strings.TrimSpace(newTop.Nr) != "" {
		oldTop.Nr = newTop.Nr
	}
	if strings.TrimSpace(newTop.Betreff) != "" {
		oldTop.Betreff = newTop.Betreff
	}
	oldTop.IndexTop = newTop.IndexTop
	oldTop.SavedAt = time.Now()

	oldTop.Datum = newTop.Datum
	oldTop.Gremium = newTop.Gremium

	return oldTop
}

func (s *Sitzung) UpdateAnlage(oldAnlage *Anlage, newAnlage *Anlage) *Anlage {
	oldAnlage.Title = newAnlage.Title
	return oldAnlage
}
func (s *Sitzung) SaveOrUpdate() error {
	tx, err := s.app.Db().NewTransaction(s.app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	var oldSitzung Sitzung
	err = tx.Get(s.GetKey(), &oldSitzung)
	if err != nil && err != datastore.ErrNoSuchEntity {
		return err
	} else if err == nil {
		//update
		//v.VOLFDNR = oldVorlage.VOLFDNR
	}

	_, err = tx.Put(s.GetKey(), s)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error saving to db sitzung from %s", s.file.GetName()))
	}
	_, err = tx.Commit()
	return err
}

func (s *Sitzung) Delete() error {

	ks, err := s.app.Db().GetAll(s.app.Ctx(), datastore.NewQuery(s.app.Config.GetEntityAnlage()).Ancestor(s.GetKey()).KeysOnly(), nil)
	if err != nil {
		return errors.Wrap(err, "error getting anlagen from db")
	}

	tks, err := s.app.Db().GetAll(s.app.Ctx(), s.GetTopQuery().KeysOnly(), nil)
	if err != nil {
		return errors.Wrap(err, "error getting tops from db")
	}

	tx, err := s.app.Db().NewTransaction(s.app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	err = tx.DeleteMulti(ks)
	if err != nil {
		slog.Error("error delete anlagen of sitzung in db for %s: %v", s.file.GetName(), err)
	}

	err = tx.DeleteMulti(tks)
	if err != nil {
		slog.Error("error delete tops of sitzung in db for %s: %v", s.file.GetName(), err)
	}

	err = tx.Delete(s.GetKey())
	if err != nil {
		slog.Error("error delete sitzung in db for %s: %v", s.file.GetName(), err)
	}

	_, err = tx.Commit()

	return err
}
