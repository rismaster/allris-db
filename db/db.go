package db

import (
	"github.com/rismaster/allris-common/application"
	"github.com/rismaster/allris-common/common/files"
	"github.com/rismaster/allris-common/common/slog"
)

func DeleteTop(app *application.AppContext, filename string) {

	file := files.NewFileFromStore(app, app.Config.GetTopFolder(), filename)
	top, err := NewTop(app, file)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

	err = top.Delete()
	if err != nil {
		slog.Fatal("err: %+v", err)
	}
}

func DeleteSitzung(app *application.AppContext, filename string) {

	file := files.NewFileFromStore(app, app.Config.GetSitzungenFolder(), filename)
	sitzung, err := NewSitzung(app, file)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

	err = sitzung.Delete()
	if err != nil {
		slog.Fatal("err: %+v", err)
	}
}

func DeleteVorlage(app *application.AppContext, filename string) {

	file := files.NewFileFromStore(app, app.Config.GetVorlagenFolder(), filename)
	vorlage, err := NewVorlage(app, file)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

	err = vorlage.Delete()
	if err != nil {
		slog.Fatal("err: %+v", err)
	}
}

func UpdateVorlage(app *application.AppContext, filename string) {

	file := files.NewFileFromStore(app, app.Config.GetVorlagenFolder(), filename)
	vorlage, err := NewVorlage(app, file)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

	err = Sync(app, vorlage)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}
}

func UpdateTop(app *application.AppContext, filename string) {

	file := files.NewFileFromStore(app, app.Config.GetTopFolder(), filename)
	top, err := NewTop(app, file)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

	err = Sync(app, top)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

}

func UpdateSitzung(app *application.AppContext, filename string) {

	file := files.NewFileFromStore(app, app.Config.GetSitzungenFolder(), filename)
	sitzung, err := NewSitzung(app, file)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

	err = Sync(app, sitzung)
	if err != nil {
		slog.Fatal("err: %+v", err)
	}

}
