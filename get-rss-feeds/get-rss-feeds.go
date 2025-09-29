// Package get-rss-feeds : Gets all RSS Feeds and the associated data from the IBM Cloudant DB
package getrssfeeds

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/IBM/cloudant-go-sdk/cloudantv1"
)

type RssFeed struct {
	Id                 string `json:"_id"`
	RssFeedName        string `json:"RSS_Feed_Name"`
	RssFeedUrl         string `json:"RSS_Feed_URL"`
	LastUpdatedDate    string `json:"Last_Updated_Date"`
	Magazine           string `json:"Magazine"`
	Language           string `json:"Language"`
	PauseIngestion     bool   `json:"Pause_Ingestion"`
	ErrorCount         int64  `json:"Error_Count"`
	PauseTimestamp     string `json:"Pause_Timestamp`
	PauseReason        string `json:"Pause_Reason"`
	PauseStopReminders bool   `json:"Pause_Stop_Reminders"`
}

type Feed struct {
	Publisher          string `json:"publisher"`
	FeedUrl            string `json:"feed_url"`
	LastUpdatedDate    string `json:"last_updated_date"`
	FeedName           string `json:"feed_name"`
	Language           string `json:"language"`
	ErrorCount         int64  `json:"error_count"`
	PauseTimestamp     string `json:"Pause_Timestamp`
	PauseReason        string `json:"Pause_Reason"`
	PauseStopReminders bool   `json:"Pause_Stop_Reminders"`
}

func GetRSSFeeds(service *cloudantv1.CloudantV1, dbName string) ([]Feed, error) {

	// selector= {"_id": {"$gt": "0"},"Publisher_Name": {"$exists": True},"RSS_Feeds": {"$exists": True}},
	selector := map[string]interface{}{
		"_id": map[string]interface{}{
			"$gt": "0",
		},
		"Publisher_Name": map[string]interface{}{
			"$exists": true,
		},
		"RSS_Feeds": map[string]interface{}{
			"$exists": true,
		},
	}

	queryOptions := &cloudantv1.PostFindOptions{
		Db:       &dbName,
		Selector: selector,
	}

	// Execute the query
	findResult, _, err := service.PostFind(queryOptions)
	if err != nil {
		fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Finding All Documents using Cloudant Service: %s\n", err)
		return nil, err
	}

	// Parse Result from Cloudant to build slice of RSS Feeds
	var feeds []Feed
	for _, doc := range findResult.Docs {
		var rssFeeds []RssFeed
		b, err := json.Marshal(doc.GetProperty("RSS_Feeds"))
		if err != nil {
			fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Marshaling RSS_Feeds interface into JSON: %s\n", err)
			return nil, err
		}
		err = json.Unmarshal(b, &rssFeeds)
		if err != nil {
			fmt.Fprintf(os.Stderr, os.Getenv("env")+" Error Decoding JSON: %s\n", err)
			return nil, err
		}
		for _, rssfeed := range rssFeeds {
			if rssfeed.PauseIngestion == true {
				continue
			}
			feed := Feed{
				Publisher:       doc.GetProperty("Publisher_Name").(string),
				FeedUrl:         rssfeed.RssFeedUrl,
				FeedName:        rssfeed.RssFeedName,
				LastUpdatedDate: rssfeed.LastUpdatedDate,
				Language:        rssfeed.Language,
				ErrorCount:      rssfeed.ErrorCount,
			}
			feeds = append(feeds, feed)
		}
	}

	return feeds, nil
}
