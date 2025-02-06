package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jaytaylor/html2text"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mmcdole/gofeed"

	"gopkg.in/yaml.v3"
)

const (
	rssRecordsTable = "rss_records"
)

var (
	acceptedContentTypes = []string{
		"text/html",
		"text/plain",
		"text/markdown",
		"application/pdf",
	}

	contentTypeExtensions = map[string]string{
		"text/html":       ".html",
		"text/plain":      ".txt",
		"text/markdown":   ".md",
		"application/pdf": ".pdf",
	}
)

type ConfigMain struct {
	DBFile     string               `yaml:"db_file"`
	ContentDir string               `yaml:"content_dir"`
	OpenWebUI  *ConfigOpenWebUI     `yaml:"open-webui"`
	RSS        []*ConfigRSSEndpoint `yaml:"rss"`
}

type ConfigOpenWebUI struct {
	APIEndpoint string `yaml:"api_endpoint"`
	APIToken    string `yaml:"api_token"`
}

type ConfigRSSEndpoint struct {
	Id              string `yaml:"id"`
	Name            string `yaml:"name"`
	Url             string `yaml:"url"`
	DataInLink      bool   `yaml:"data_in_link"`
	AuthorOverride  string `yaml:"author_override"`
	HtmlToMarkdown  bool   `yaml:"html_to_markdown"`
	KnowledgeBaseId string `yaml:"owui_knowledge_base"`
}

type KnowledgeAddBody struct {
	FileId string `json:"file_id"`
}

func main() {
	config := loadConfig()

	// Instantiate sqlite DB connection
	db, err := sql.Open("sqlite3", config.DBFile)
	if err != nil {
		log.Panicf("Error opening database: %v", err)
	}
	defer db.Close()

	createRSSRecordsTable(db)

	fp := gofeed.NewParser()

	for _, rssEndpoint := range config.RSS {
		feed, err := fp.ParseURL(rssEndpoint.Url)
		if err != nil {
			log.Printf("Error parsing URL: %v", err)
			continue
		}

		for _, rssItem := range feed.Items {
			fileName := ""
			var content *[]byte
			contentType := ""

			hash := findItemInDB(db, rssEndpoint.Id, rssItem.GUID)
			if hash != "" {
				// Continue if item is already in DB
				continue
			}

			hash = genItemHash(rssEndpoint.Id, rssItem.GUID)

			if rssEndpoint.DataInLink {
				// Travel to link and retrieve body

				content, contentType, err = fetchContent(rssItem.Link)
				if err != nil {
					log.Printf("Error fetching content: %v", err)
					continue
				}

				// Optionally convert html to markdown (might make it more legible to the embedder)
				if contentType == "text/html" && rssEndpoint.HtmlToMarkdown {
					markdown, err := html2text.FromString(string(*content), html2text.Options{
						PrettyTables: true,
						OmitLinks:    true,
					})
					if err != nil {
						log.Printf("Error converting HTML to markdown: %v", err)
						continue
					}

					newContent := []byte(markdown)
					content = &newContent
					contentType = "text/markdown"
				}

				// Cache data on filesystem
				filePath := fmt.Sprintf("%s/%s %s %s%s", config.ContentDir, rssEndpoint.Name, rssItem.PublishedParsed.Format("2006-01-02 15:04:05"), hash[0:6], contentTypeExtensions[contentType])
				fh, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Printf("Error opening file: %v", err)
					continue
				}
				defer fh.Close()
				fileName = fh.Name()
				_, err = fh.Write(*content)
				if err != nil {
					log.Printf("Error writing file: %v", err)
					continue
				}
			} else {
				// Create a wee markdown file when not following links

				if rssItem.Description == "<p></p>" || rssItem.Title == "" {
					// For these purposes, we're only interested in filled-out text
					// not images or lone links
					continue
				}
				if strings.Contains(rssItem.Title, "[No Title]") {
					// This one's a quirk from _certain_ sites
					continue
				}

				filePath := fmt.Sprintf("%s/%s %s %s.md", config.ContentDir, rssEndpoint.Name, rssItem.PublishedParsed.Format("2006-01-02 15:04:05"), hash[0:6])

				fh, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Printf("Error opening file: %v", err)
					continue
				}
				defer fh.Close()
				fileName = fh.Name()
				metadataMd := ""

				// Fill out link, if any
				if rssItem.Link != "" {
					metadataMd += fmt.Sprintf("* **Link**: %s\n", rssItem.Link)
				}

				// Fill out author field
				if rssEndpoint.AuthorOverride != "" {
					metadataMd += fmt.Sprintf("* **Author**: %s\n", rssEndpoint.AuthorOverride)
				} else {
					for _, a := range rssItem.Authors {
						metadataMd += fmt.Sprintf("* **Author**: %s\n", a.Name)
					}
				}

				body := fmt.Sprintf("# %s [%s]\n\n## %s\n\n%s\n%s", rssEndpoint.Name, feed.Description, rssItem.Published, metadataMd, rssItem.Description)

				_, err = fh.WriteString(body)
				if err != nil {
					log.Printf("Error writing file: %v", err)
					continue
				}

				bodyBytes := []byte(body)
				content = &bodyBytes
			}

			if fileName != "" && content != nil {
				err = owuiSendKnowledge(config.OpenWebUI, rssEndpoint.KnowledgeBaseId, fileName, content)
				if err != nil {
					log.Printf("Error sending knowledge to open-webui: %v", err)
					continue
				}
				log.Printf("Successfully added %s to knowledge base %s", fileName, rssEndpoint.KnowledgeBaseId)

				err = recordItemInDB(db, rssEndpoint.Id, rssItem.GUID, hash)
				if err != nil {
					log.Printf("Error recording item in DB: %v", err)
					continue
				}

				time.Sleep(time.Second * 5)
			}
		}
	}
}

func loadConfig() *ConfigMain {
	var err error

	fh, err := os.Open("config.yml")
	if err != nil {
		log.Panicf("Error opening config.yml: %v", err)
	}
	defer fh.Close()

	yamlBytes, err := io.ReadAll(fh)
	if err != nil {
		log.Panicf("Error reading bytes from config.yml: %v", err)
	}

	config := ConfigMain{}
	err = yaml.Unmarshal(yamlBytes, &config)
	if err != nil {
		log.Panicf("Error unmarshaling config.yml: %v", err)
	}

	return &config
}

func createRSSRecordsTable(db *sql.DB) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name=?;`, rssRecordsTable)
	if err != nil {
		log.Panicf("Error querying database: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// No table yet exists

		createStmt := fmt.Sprintf("create table %s (rss_id text not null, guid text not null, hash text not null, PRIMARY KEY (rss_id, guid), UNIQUE(hash));", rssRecordsTable)
		_, err = db.Exec(createStmt)
		if err != nil {
			log.Panicf("Error creating table: %v", err)
		}
	}
}

func genItemHash(rssId string, guid string) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s-%s", rssId, guid)))
	hashStr := fmt.Sprintf("%x", hash)

	return hashStr
}

func findItemInDB(db *sql.DB, rssId string, guid string) string {
	var err error
	rows, err := db.Query(fmt.Sprintf("SELECT hash FROM %s WHERE rss_id=? AND guid=?;", rssRecordsTable), rssId, guid)
	if err != nil {
		log.Printf("Error querying database: %v", err)
		return ""
	}
	defer rows.Close()

	if !rows.Next() {
		return ""
	}

	var hash string

	err = rows.Scan(&hash)
	if err != nil {
		log.Printf("Error scanning row: %v", err)
		return ""
	}

	return hash
}

func fetchContent(url string) (*[]byte, string, error) {
	hc := http.Client{}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	for _, t := range acceptedContentTypes {
		req.Header.Add("Accept", t)
	}
	req.Header.Add("User-Agent", "Mozilla/5.0")

	res, err := hc.Do(req)
	if err != nil {
		return nil, "", err
	}

	if res.StatusCode != 200 {
		return nil, "", errors.New("non-200 response code")
	}

	resContent := res.Header.Get("Content-Type")
	contentType := "text/html"
	foundAccepted := false
	for _, t := range acceptedContentTypes {
		if strings.Contains(resContent, t) {
			foundAccepted = true
			contentType = t
			break
		}
	}
	if !foundAccepted {
		return nil, "", errors.New("unreadable response")
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()

	return &body, contentType, nil
}

func owuiSendKnowledge(config *ConfigOpenWebUI, knowledgeBaseId string, fileName string, content *[]byte) error {
	fileUploadEndpoint := fmt.Sprintf("%s/v1/files/", config.APIEndpoint)
	knowledgeLinkEndpoint := fmt.Sprintf("%s/v1/knowledge/%s/file/add", config.APIEndpoint, knowledgeBaseId)

	hc := http.Client{}

	// PART 1: File upload

	// Build multipart form with file contents
	var body bytes.Buffer
	mpW := multipart.NewWriter(&body)
	fw, err := mpW.CreateFormFile("file", fileName)
	if err != nil {
		return err
	}
	fw.Write(*content)
	mpW.Close()

	// Send data to Open-WebUI
	req, err := http.NewRequest(http.MethodPost, fileUploadEndpoint, &body)
	if err != nil {
		return err
	}
	// Must load Content-Type from multipart form to include boundary
	req.Header.Add("Content-Type", mpW.FormDataContentType())
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", config.APIToken))

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// Parse the body so we can grab the file ID later
	respBodyContents, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	respBody := map[string]any{}
	err = yaml.Unmarshal(respBodyContents, &respBody)
	if err != nil {
		return err
	}

	// PART 2: Link to knowledge base
	knowledge := KnowledgeAddBody{
		FileId: respBody["id"].(string),
	}
	knowledgeBytes, err := json.Marshal(knowledge)
	if err != nil {
		return err
	}

	req, err = http.NewRequest(http.MethodPost, knowledgeLinkEndpoint, bytes.NewReader(knowledgeBytes))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", config.APIToken))

	resp, err = hc.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return errors.New("non-200 response code")
	}

	return nil
}

func recordItemInDB(db *sql.DB, rssId string, guid string, hash string) error {
	res, err := db.Exec(fmt.Sprintf("insert into %s (rss_id, guid, hash) values (?, ?, ?);", rssRecordsTable), rssId, guid, hash)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return errors.New("no rows affected")
	}

	return nil
}
