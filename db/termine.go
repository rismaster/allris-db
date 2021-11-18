package db

import (
	"bytes"
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/kennygrant/sanitize"
	"github.com/pkg/errors"
	"github.com/rismaster/allris-common/application"
	"github.com/rismaster/allris-common/common/db"
	"github.com/rismaster/allris-common/common/files"
	"github.com/rismaster/allris-common/common/slog"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Termin struct {
	Gremium string
	SILFDNR int
	Start   time.Time
	End     time.Time
	file    files.File

	SavedAt time.Time
}

func UpdateTermine(app *application.AppContext, minDate time.Time) error {

	f := files.NewFileFromStore(app, "", app.Config.GetAlleSitzungenType()+".html")
	err := f.ReadDocument(app.Config.GetBucketFetched())
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error reading file %s", app.Config.GetAlleSitzungenType()))
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(f.GetContent()))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error create dom from %s", f.GetName()))
	}

	termine, err := parseTerminList(app, doc)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error parsing dom from %s", f.GetName()))
	}

	if len(termine) < 1 {
		return errors.New("empty termine")
	}

	var tmap = make(map[string]bool)
	var terminKeys []*datastore.Key
	for _, termin := range termine {
		keyName := sanitize.Path(termin.Gremium + "_" + termin.Start.Format(app.Config.GetDateFormatTech()))
		key := datastore.NameKey(app.Config.GetEntityTermin(), keyName, nil)
		exist := tmap[key.Encode()]
		if !exist && termin.Start.After(minDate) {

			tmap[key.Encode()] = true
			terminKeys = append(terminKeys, key)
		}
	}

	qberdel := datastore.NewQuery(app.Config.GetEntityTermin()).Filter("Start > ", minDate).KeysOnly()

	oldKeys, err1 := app.Db().GetAll(app.Ctx(), qberdel, nil)
	if err1 != nil {
		return errors.Wrap(err1, "error getting termine from db")
	}

	var kstodelete []*datastore.Key
	for _, k := range oldKeys {
		exist := tmap[k.Encode()]
		if !exist {
			kstodelete = append(kstodelete, k)
		}
	}

	err1 = db.DoInBatch(500, len(kstodelete), func(i int, j int) error {
		slog.Info("delete %d termine", j-i)
		return app.Db().DeleteMulti(app.Ctx(), kstodelete[i:j])
	})
	if err1 != nil {
		return errors.Wrap(err1, "error delete old termine from db")
	}

	err1 = db.DoInBatch(500, len(terminKeys), func(i int, j int) error {
		for _, tk := range terminKeys[i:j] {
			slog.Info(tk.Name)
		}
		slog.Info("save %d termine", j-i)
		_, err2 := app.Db().PutMulti(app.Ctx(), terminKeys[i:j], termine[i:j])
		return err2
	})
	if err1 != nil {

		slog.Fatal("err: %+v", err)
	}

	return nil
}

func parseTerminList(app *application.AppContext, doc *goquery.Document) (termine []Termin, err error) {

	selector := "tr.zl11,tr.zl12"
	doc.Find(selector).Each(func(index int, selection *goquery.Selection) {

		if selection.Children().Size() >= 8 {

			sitzung, lastErr := parseTermin(app, selection)
			if lastErr == nil {
				termine = append(termine, *sitzung)
			} else {
				err = lastErr
			}
		}
	})
	return termine, err
}

func parseTermin(app *application.AppContext, e *goquery.Selection) (*Termin, error) {

	lnkTr := e.Find(":nth-child(2) a")
	lnk, _ := lnkTr.Attr("href")
	lnkUrlAttr, err := url.Parse(lnk)
	if err != nil {
		return nil, err
	}

	silfdnr := lnkUrlAttr.Query().Get("SILFDNR")
	name := strings.TrimSpace(lnkTr.First().Text())
	dateText := strings.TrimSpace(e.Find(":nth-child(6) a").Text())
	timeTr := strings.Split(strings.TrimSpace(e.Find(":nth-child(7)").Text()), " - ")
	dateTimetxt := fmt.Sprintf("%s %s:00", dateText, timeTr[0])

	localTz, _ := time.LoadLocation(app.Config.GetTimezone())
	startTime, err := time.ParseInLocation(app.Config.GetDateFormatWithTime(), dateTimetxt, localTz)
	if err != nil {
		return nil, err
	}

	var endTime = startTime
	if len(timeTr) > 1 && len(timeTr[1]) > 0 {

		endDateTxt := fmt.Sprintf("%s %s:00", dateText, timeTr[1])
		endTime, err = time.ParseInLocation(app.Config.GetDateFormatWithTime(), endDateTxt, localTz)
		if err != nil {
			return nil, err
		}
	}

	var silfdnrInt = 0

	if silfdnr != "" {
		sint, err1 := strconv.Atoi(silfdnr)
		if err1 != nil {
			return nil, errors.Wrap(err1, "cannot create int from silfdnr")
		}
		silfdnrInt = sint
	} else {

		name = e.Find(":nth-child(2)").Text()
	}

	return &Termin{
		Gremium: name,
		Start:   startTime,
		End:     endTime,
		SILFDNR: silfdnrInt,
		SavedAt: time.Now(),
	}, nil
}
