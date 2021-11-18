package db

import (
	"bytes"
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
	"github.com/rismaster/allris-common/application"
	"github.com/rismaster/allris-common/common/files"
	"github.com/rismaster/allris-common/common/slog"
	"time"
)

type TopHolder interface {
	GetFile() *files.File
	Parse(doc *goquery.Document) error
	SetSavedAt(time time.Time)
	GetTops() []*Top
	GetAnlagen() []*Anlage
	GetKey() *datastore.Key
	UpdateTop(*Top, *Top) *Top
	UpdateAnlage(*Anlage, *Anlage) *Anlage
	GetTopQuery() *datastore.Query
	GetDirectAnlagenQuery() *datastore.Query
	SaveOrUpdate() error
}

type HasKey interface {
	GetKey() *datastore.Key
}

func Sync(app *application.AppContext, s TopHolder) error {

	file := s.GetFile()

	err := file.ReadDocument(app.Config.GetBucketFetched())
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error reading file %s (%s)", file.GetPath(), app.Config.GetAlleSitzungenType()))
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(file.GetContent()))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error create dom from %s", file.GetName()))
	}

	err = s.Parse(doc)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error parsing sitzung from %s", file.GetName()))
	}

	s.SetSavedAt(time.Now())

	///
	if s.GetTopQuery() != nil {

		err = saveTops(app, s, err)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error saving top from %s", file.GetName()))
		}
	}

	err = saveAnlagen(app, s, err)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error saving anlagen from %s", file.GetName()))
	}

	return s.SaveOrUpdate()
}

func saveTops(app *application.AppContext, s TopHolder, err error) error {
	newTopsMap := make(map[string]*Top)
	for _, t := range s.GetTops() {
		newTopsMap[t.GetKey().Encode()] = t
	}

	ks, err := app.Db().GetAll(app.Ctx(), s.GetTopQuery().KeysOnly(), nil)
	if err != nil {
		return errors.Wrap(err, "error getting beratungen from db")
	}

	tx, err := app.Db().NewTransaction(app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	oldTops := make([]*Top, len(ks))
	err = tx.GetMulti(ks, oldTops)
	if err != nil {
		return errors.Wrap(err, "error getting beratungen from db")
	}

	for i, oldTop := range oldTops {
		oldkey := ks[i]
		kstr := oldTop.GetKey().Encode()
		newTop, exist := newTopsMap[kstr]
		if !exist {
			err = tx.Delete(oldkey)
			if err != nil {
				slog.Error("delete old top %s: %v", newTop.GetKey().String(), err)
			}
		} else {
			_, err = tx.Put(newTop.GetKey(), s.UpdateTop(oldTop, newTop))
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("put new top %s", newTop.GetKey().String()))
			}
			delete(newTopsMap, kstr)
		}
	}

	for _, newTop := range newTopsMap {
		_, err = tx.Put(newTop.GetKey(), newTop)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("put new top %s", newTop.GetKey().String()))
		}
	}

	_, err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error commiting to db sitzung from %s", s.GetKey()))
	}

	return nil
}

func saveAnlagen(app *application.AppContext, s TopHolder, err error) error {
	newTopsMap := make(map[string]*Anlage)
	for _, t := range s.GetAnlagen() {
		newTopsMap[t.GetKey(s.GetKey()).Encode()] = t
	}

	ks, err := app.Db().GetAll(app.Ctx(), s.GetDirectAnlagenQuery().KeysOnly(), nil)
	if err != nil {
		return errors.Wrap(err, "error getting beratungen from db")
	}

	tx, err := app.Db().NewTransaction(app.Ctx())
	if err != nil {
		return errors.Wrap(err, "client.NewTransaction")
	}

	oldTops := make([]*Anlage, len(ks))
	err = tx.GetMulti(ks, oldTops)
	if err != nil {
		return errors.Wrap(err, "error getting beratungen from db")
	}

	for i, oldTop := range oldTops {
		oldkey := ks[i]
		kstr := oldTop.GetKey(s.GetKey()).Encode()
		newTop, exist := newTopsMap[kstr]
		if !exist {
			err = tx.Delete(oldkey)
			if err != nil {
				slog.Error("delete old top %s: %v", newTop.GetKey(s.GetKey()).String(), err)
			}
		} else {
			_, err = tx.Put(newTop.GetKey(s.GetKey()), s.UpdateAnlage(oldTop, newTop))
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("put new top %s", newTop.GetKey(s.GetKey()).String()))
			}
			delete(newTopsMap, kstr)
		}
	}

	for _, newTop := range newTopsMap {
		_, err = tx.Put(newTop.GetKey(s.GetKey()), newTop)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("put new top %s", newTop.GetKey(s.GetKey()).String()))
		}
	}

	_, err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error commiting to db sitzung from %s", s.GetKey()))
	}

	return nil
}
