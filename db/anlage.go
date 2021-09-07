package db

import (
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/kennygrant/sanitize"
	allris_common "github.com/rismaster/allris-common"
	"github.com/rismaster/allris-common/application"
	"github.com/rismaster/allris-common/common/domtools"
	"github.com/rismaster/allris-common/common/files"
	"regexp"
	"strconv"
	"time"
)

type Anlage struct {
	SILFDNR  int
	TOLFDNR  int
	VOLFDNR  int
	DOLFDNR  int
	Type     string
	Filename string

	Title string

	SavedAt time.Time

	parent TopHolder
	Config allris_common.Config
}

var RegexTopAnlage = regexp.MustCompile(`sitzung-([0-9]+)-top-([0-9]+)-anlage-(.+)`)
var RegexAnlagen = regexp.MustCompile(`(vorlage|sitzung)-([0-9]+)-(basisanlage|anlage)-(([0-9]+)-(.+))`)

func (a *Anlage) GetKey(parentKey *datastore.Key) *datastore.Key {
	kn := fmt.Sprintf("%d_%d_%d_%d_%s", a.DOLFDNR, a.SILFDNR, a.TOLFDNR, a.VOLFDNR, a.Title)
	return datastore.NameKey(a.Config.GetEntityAnlage(), sanitize.Name(kn), parentKey)
}

func NewAnlage(app *application.AppContext, file *files.File) (*Anlage, error) {

	filename := file.GetPath()
	var silfdnr = 0
	var volfdnr = 0
	var dolfdnr = 0
	var tolfdnr = 0
	var anlageType = ""

	if RegexTopAnlage.MatchString(filename) {
		matches := RegexTopAnlage.FindStringSubmatch(filename)

		sil, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, err
		}
		tol, err := strconv.Atoi(matches[2])
		if err != nil {
			return nil, err
		}

		silfdnr = sil
		tolfdnr = tol
		anlageType = app.Config.GetAnlageType()

	} else if RegexAnlagen.MatchString(filename) {
		matches := RegexAnlagen.FindStringSubmatch(filename)

		if matches[1] == app.Config.GetSitzungType() {

			sil, err := strconv.Atoi(matches[2])
			if err != nil {
				return nil, err
			}
			silfdnr = sil
		} else if matches[1] == app.Config.GetVorlageType() {

			vol, err := strconv.Atoi(matches[2])
			if err != nil {
				return nil, err
			}
			volfdnr = vol
		}

		anlageType = matches[3]

		if anlageType == app.Config.GetAnlageDocumentType() {

			dol, err := strconv.Atoi(matches[5])
			if err != nil {
				return nil, err
			}

			dolfdnr = dol
		}
	}

	return &Anlage{
		SILFDNR:  silfdnr,
		TOLFDNR:  tolfdnr,
		DOLFDNR:  dolfdnr,
		VOLFDNR:  volfdnr,
		Filename: filename,
		Type:     anlageType,
		Config:   app.Config,
	}, nil
}

func ExtractAnlagen(dom *goquery.Selection, config allris_common.Config) (docs []*Anlage) {

	theAnlagenTables := dom.Find("table.tk1")
	if theAnlagenTables.Size() <= 1 {
		return docs
	}

	trs := theAnlagenTables.Last().Find("tr")
	if trs.Size() < 2 || trs.Next().Children().Size() < 2 {
		return docs
	}

	trs.Each(func(i int, selection *goquery.Selection) {
		tds := selection.Find("td")
		if i > 2 && tds.Size() >= 3 {
			lnk := tds.Get(2).FirstChild
			if lnk != nil {
				description := domtools.GetChildTextFromNode(lnk)
				doc := &Anlage{
					Title:  description,
					Type:   config.GetAnlageType(),
					Config: config,
				}
				docs = append(docs, doc)
			}
		}
	})
	return docs
}

func ExtractBasisAnlagen(dom *goquery.Selection, config allris_common.Config) (docs []*Anlage) {

	theTopTable := dom.Find(".me1 > table.tk1").First()
	selector := "form[action=\"" + config.GetUrlAnlagedoc() + "\"]"
	var form = theTopTable.Find(selector)
	for ; form.Nodes != nil; form = form.NextFiltered(selector) {
		dolfdnr := domtools.ExtractIntFromInput(form, "DOLFDNR")

		title, _ := form.Find("input[type=\"submit\"]").Attr("title")

		docs = append(docs, &Anlage{
			Title:   title,
			DOLFDNR: dolfdnr,
			Type:    config.GetAnlageDocumentType(),
			Config:  config,
		})

	}
	return docs
}
