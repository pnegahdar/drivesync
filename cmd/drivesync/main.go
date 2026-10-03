// drivesync is a small manual-testing CLI. The server's bearer authenticator is
// deliberately single-principal; production embedders supply their own hook.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	ds "github.com/pnegahdar/drivesync"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: drivesync serve|create|attach|status [flags]")
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	url := flags.String("url", "http://127.0.0.1:8787", "server URL")
	data := flags.String("data", "./drivesync-data", "server data directory")
	addr := flags.String("addr", "127.0.0.1:8787", "server listen address")
	tenant := flags.String("tenant", "local", "principal tenant")
	subject := flags.String("subject", "user", "principal subject")
	name := flags.String("name", "shared", "folder or replica name")
	description := flags.String("description", "", "folder description")
	folder := flags.String("folder", "", "folder ID")
	keyFile := flags.String("key-file", "", "file containing the 32-byte key in hex; otherwise DRIVESYNC_KEY")
	dir := flags.String("dir", "./shared", "attachment directory")
	state := flags.String("state", "", "replica state directory, outside attachment")
	maxFile := flags.Int64("max-file", 0, "maximum sealed bytes per entry, 0 unlimited")
	maxTotal := flags.Int64("max-total", 0, "maximum sealed bytes in folder, 0 unlimited")
	maxFiles := flags.Int64("max-files", 0, "maximum entries, 0 unlimited")
	maxRows := flags.Int64("max-rows", 0, "maximum rows including tombstones and garbage; required for sharing")
	_ = flags.Parse(os.Args[2:])
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	token := os.Getenv("DRIVESYNC_TOKEN")
	printJSON := func(v any) {
		if e := json.NewEncoder(os.Stdout).Encode(v); e != nil {
			log.Fatal(e)
		}
	}
	if command == "status" {
		if *state != "" {
			b, e := os.ReadFile(filepath.Join(*state, "status.json"))
			if e != nil {
				log.Fatal(e)
			}
			fmt.Println(string(b))
			return
		}
		if token == "" {
			log.Fatal("set DRIVESYNC_TOKEN")
		}
		c := ds.NewHTTPClient(*url, http.Header{"Authorization": {"Bearer " + token}})
		folders, e := c.ListFolders(ctx)
		if e != nil {
			log.Fatal(e)
		}
		printJSON(folders)
		return
	}
	if token == "" {
		log.Fatal("set a nonempty DRIVESYNC_TOKEN")
	}
	if command == "serve" {
		if e := os.MkdirAll(*data, 0700); e != nil {
			log.Fatal(e)
		}
		meta, e := ds.OpenSQLiteMetaStore(filepath.Join(*data, "meta.sqlite"))
		if e != nil {
			log.Fatal(e)
		}
		defer meta.Close()
		blobs, e := ds.OpenDirectoryBlobStore(filepath.Join(*data, "blobs"))
		if e != nil {
			log.Fatal(e)
		}
		defer blobs.Close()
		server := ds.NewServer(meta, blobs)
		if e = server.RecoverUploads(ctx); e != nil {
			log.Fatal(e)
		}
		gcCtx, stopGC := context.WithCancel(ctx)
		gcDone := make(chan struct{})
		go func() { defer close(gcDone); server.RunGC(gcCtx, time.Second) }()
		defer func() { stopGC(); <-gcDone }()
		httpServer := &http.Server{Addr: *addr, Handler: server.Handler(func(r *http.Request) (ds.Principal, error) {
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
				return ds.Principal{}, ds.ErrDenied
			}
			return ds.Principal{Tenant: *tenant, Subject: *subject}, nil
		}), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(shutdown)
		}()
		log.Printf("listening on %s", *addr)
		if e = httpServer.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
			log.Fatal(e)
		}
		return
	}
	c := ds.NewHTTPClient(*url, http.Header{"Authorization": {"Bearer " + token}})
	keyHex := os.Getenv("DRIVESYNC_KEY")
	if *keyFile != "" {
		b, e := os.ReadFile(*keyFile)
		if e != nil {
			log.Fatal(e)
		}
		keyHex = strings.TrimSpace(string(b))
	}
	var key ds.FolderKey
	if keyHex == "" && command == "create" {
		key = ds.NewFolderKey()
	} else {
		b, e := hex.DecodeString(keyHex)
		if e != nil || len(b) != 32 {
			log.Fatal("-key-file or DRIVESYNC_KEY must contain 64 hex characters")
		}
		copy(key[:], b)
	}
	switch command {
	case "create":
		f, e := c.CreateFolder(ctx, ds.FolderSpec{Name: *name, Description: *description, Limits: ds.Limits{MaxFileBytes: *maxFile, MaxTotalBytes: *maxTotal, MaxFiles: *maxFiles, MaxRows: *maxRows}, KeyCheck: ds.KeyCheck(key)})
		if e != nil {
			log.Fatal(e)
		}
		printJSON(struct {
			Folder ds.Folder
			Key    string
		}{f, hex.EncodeToString(key[:])})
	case "attach":
		r, e := ds.Attach(ctx, c, *folder, key, *dir, ds.Options{Name: *name, StateDir: *state})
		if e != nil {
			log.Fatal(e)
		}
		defer r.Close()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				printJSON(r.Status())
				return
			case <-ticker.C:
				printJSON(r.Status())
			}
		}
	default:
		log.Fatal("unknown command")
	}
}
