package config

import (
	"testing"
)

func TestLoadDefaultsAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	var c Dispatcher
	if err := Load(&c); err != nil {
		t.Fatal(err)
	}
	if c.GRPCAddr != ":9090" || c.Concurrency != 32 || c.DBMaxConns != 20 {
		t.Fatalf("defaults not applied: %+v", c)
	}

	t.Setenv("LOG_LEVEL", "loud")
	if err := Load(&c); err == nil {
		t.Fatal("want error for bad LOG_LEVEL")
	}
}

func TestRequiredDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	var c Ingest
	if err := Load(&c); err == nil {
		t.Fatal("want error for missing DATABASE_URL")
	}
}

func TestMasterKey(t *testing.T) {
	if _, err := (Secrets{MasterKey: "short"}).MasterKeyBytes(); err == nil {
		t.Fatal("want error")
	}
	k, err := (Secrets{MasterKey: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}).MasterKeyBytes()
	if err != nil || len(k) != 32 {
		t.Fatalf("got %v %v", len(k), err)
	}
}
