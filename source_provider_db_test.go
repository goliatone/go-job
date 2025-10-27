package job_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/goliatone/go-job"
	_ "github.com/mattn/go-sqlite3"
)

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	// Create an in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}

	// Create the test table
	_, err = db.Exec(`
		CREATE TABLE scripts (
			path TEXT PRIMARY KEY,
			content BLOB
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

func insertTestScript(t *testing.T, db *sql.DB, path string, content []byte) {
	t.Helper()

	_, err := db.Exec("INSERT INTO scripts (path, content) VALUES (?, ?)", path, content)
	if err != nil {
		t.Fatalf("Failed to insert test script: %v", err)
	}
}

func TestNewDBSourceProvider(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	provider := job.NewDBSourceProvider(db, "scripts").WithPlaceholder(job.SQLQuestionPlaceholder)

	if provider.DB != db {
		t.Error("Expected db to be set correctly")
	}

	if provider.Table != "scripts" {
		t.Error("Expected table to be set correctly")
	}
}

func TestDBSourceProvider_GetScript(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	provider := job.NewDBSourceProvider(db, "scripts").WithPlaceholder(job.SQLQuestionPlaceholder)

	// Test data
	testCases := []struct {
		name        string
		path        string
		content     []byte
		insertData  bool
		expectError bool
	}{
		{
			name:        "existing script",
			path:        "test/script.sql",
			content:     []byte("SELECT * FROM users;"),
			insertData:  true,
			expectError: false,
		},
		{
			name:        "non-existing script",
			path:        "test/missing.sql",
			content:     nil,
			insertData:  false,
			expectError: true,
		},
		{
			name:        "path with spaces",
			path:        "test/ script with spaces.sql",
			content:     []byte("SELECT 1;"),
			insertData:  true,
			expectError: false,
		},
		{
			name:        "empty content",
			path:        "test/empty.sql",
			content:     []byte(""),
			insertData:  true,
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.insertData {
				insertTestScript(t, db, tc.path, tc.content)
			}

			content, err := provider.GetScript(tc.path)

			if tc.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(content) != len(tc.content) {
				t.Errorf("Expected content length %d, got %d", len(tc.content), len(content))
			}

			for i, b := range content {
				if i >= len(tc.content) || tc.content[i] != b {
					t.Errorf("Content mismatch at position %d", i)
					break
				}
			}
		})
	}
}

func TestDBSourceProvider_GetScript_PathCleaning(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	provider := job.NewDBSourceProvider(db, "scripts").WithPlaceholder(job.SQLQuestionPlaceholder)

	// Insert script with clean path
	cleanPath := "clean/path.sql"
	content := []byte("SELECT 1;")
	insertTestScript(t, db, cleanPath, content)

	// Test various dirty paths that should be cleaned to the same path
	dirtyPaths := []string{
		"clean//path.sql",
		"./clean/path.sql",
		"clean/./path.sql",
		"clean/../clean/path.sql",
	}

	for _, dirtyPath := range dirtyPaths {
		t.Run(dirtyPath, func(t *testing.T) {
			result, err := provider.GetScript(dirtyPath)
			if err != nil {
				t.Errorf("Unexpected error for path %s: %v", dirtyPath, err)
				return
			}

			if len(result) != len(content) {
				t.Errorf("Content length mismatch for path %s", dirtyPath)
			}
		})
	}
}

func TestDBSourceProvider_ListScripts(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	provider := job.NewDBSourceProvider(db, "scripts").WithPlaceholder(job.SQLQuestionPlaceholder)

	// Test empty table
	t.Run("empty table", func(t *testing.T) {
		scripts, err := provider.ListScripts(context.Background())
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if len(scripts) != 0 {
			t.Errorf("Expected 0 scripts, got %d", len(scripts))
		}
	})

	// Insert test data
	testScripts := []struct {
		path    string
		content []byte
	}{
		{"path1.sql", []byte("SELECT 1;")},
		{"dir/path2.sql", []byte("SELECT 2;")},
		{"dir/subdir/path3.sql", []byte("SELECT 3;")},
	}

	for _, script := range testScripts {
		insertTestScript(t, db, script.path, script.content)
	}

	t.Run("list all scripts", func(t *testing.T) {
		scripts, err := provider.ListScripts(context.Background())
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if len(scripts) != len(testScripts) {
			t.Errorf("Expected %d scripts, got %d", len(testScripts), len(scripts))
		}

		// Create a map for easier verification
		scriptMap := make(map[string]job.ScriptInfo)
		for _, script := range scripts {
			scriptMap[script.Path] = script
		}

		// Verify each script
		for _, expected := range testScripts {
			script, exists := scriptMap[expected.path]
			if !exists {
				t.Errorf("Expected script %s not found", expected.path)
				continue
			}

			expectedID := expected.path
			if idx := len(expected.path) - 1; idx >= 0 {
				for i := idx; i >= 0; i-- {
					if expected.path[i] == '/' {
						expectedID = expected.path[i+1:]
						break
					}
				}
			}

			if script.ID != expectedID {
				t.Errorf("Expected ID %s, got %s", expectedID, script.ID)
			}

			if script.Path != expected.path {
				t.Errorf("Expected path %s, got %s", expected.path, script.Path)
			}

			if len(script.Content) != len(expected.content) {
				t.Errorf("Content length mismatch for %s", expected.path)
			}
		}
	})
}

func TestDBSourceProvider_ListScripts_ContextCancellation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	provider := job.NewDBSourceProvider(db, "scripts")

	// Insert a large number of scripts to make the operation take some time
	for i := range 100 {
		insertTestScript(t, db, fmt.Sprintf("script%d.sql", i), []byte(fmt.Sprintf("SELECT %d;", i)))
	}

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Start the operation in a goroutine
	done := make(chan bool)
	var err error
	go func() {
		_, err = provider.ListScripts(ctx)
		done <- true
	}()

	// Cancel the context immediately
	cancel()

	// Wait for the operation to complete
	select {
	case <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled error, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Operation did not complete within timeout")
	}
}

func TestDBSourceProvider_DatabaseErrors(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Close the database to simulate connection errors
	db.Close()

	provider := job.NewDBSourceProvider(db, "scripts")

	t.Run("GetScript with closed connection", func(t *testing.T) {
		_, err := provider.GetScript("test.sql")
		if err == nil {
			t.Error("Expected error with closed connection")
		}
	})

	t.Run("ListScripts with closed connection", func(t *testing.T) {
		_, err := provider.ListScripts(context.Background())
		if err == nil {
			t.Error("Expected error with closed connection")
		}
	})
}

func TestDBSourceProvider_SQLInjection(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	provider := job.NewDBSourceProvider(db, "scripts")

	// Insert legitimate script
	insertTestScript(t, db, "legitimate.sql", []byte("SELECT 1;"))

	// Try SQL injection in path
	maliciousPath := "legitimate.sql'; DROP TABLE scripts; --"

	_, err := provider.GetScript(maliciousPath)
	if err == nil {
		t.Error("Expected error for malicious path")
	}

	// Verify the table still exists and contains data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM scripts").Scan(&count)
	if err != nil {
		t.Errorf("Table might have been dropped: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 script in table, got %d", count)
	}
}
