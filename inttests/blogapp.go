package inttests

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/mongodb/mongonet/util"
	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	BlogsDB      = "blogs"
	PostsColl    = "posts"
	CommentsColl = "comments"
)

func RunProxyConnectionPerformanceBlogApp(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, mode util.MongoConnectionMode, goals []ConnectionPerformanceTestGoal, mongoClientFactory func(host string, port int, mode util.MongoConnectionMode, secondaryReads bool, appName string) (*mongo.Client, error)) error {
	for _, goal := range goals {
		if err := runProxyConnectionPerformanceBlogApp(iterations, mongoPort, proxyPort, hostname, logger, goal.Workers, goal.AvgLatencyMs, goal.MaxLatencyMs, mode, mongoClientFactory); err != nil {
			return err
		}
	}
	return nil
}

func getCommentDoc(postId, commentId int) bson.D {
	return bson.D{
		{"post_id", postId},
		{"author", bson.D{
			{"fullName", RandString(12)},
			{"dob", RandDate()},
			{"age", rand.Intn(100)},
			{"avatar", RandString(30)},
		}},
		{"upvotes", rand.Intn(100)},
		{"downvotes", rand.Intn(100)},
		{"title", RandString(30)},
		{"date", RandDate()},
	}
}

func getPostDoc(id int) bson.D {
	rand.Seed(time.Now().UnixNano())
	return bson.D{
		{"_id", id},
		{"author", bson.D{
			{"fullName", RandString(12)},
			{"dob", RandDate()},
			{"age", rand.Intn(100)},
			{"avatar", RandString(30)},
		}},
		{"upvotes", rand.Intn(100)},
		{"content", getRandStringArray(100, 30)},
	}
}

func blogPostFindOne(logger *slogger.Logger, coll *mongo.Collection, goctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	doc := bson.D{}
	cur, err := coll.Find(goctx, bson.D{{"author.age", rand.Intn(100)}})
	if err != nil {
		return err
	}
	cur.Next(goctx)
	if err := cur.Decode(&doc); err != nil {
		return err
	}
	return nil
}

func blogPostFindSome(logger *slogger.Logger, coll *mongo.Collection, goctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	var res []interface{}
	cur, err := coll.Find(goctx, bson.D{{"author.age", bson.D{{"$gt", rand.Intn(90)}}}})
	if err != nil {
		return err
	}
	return cur.All(goctx, &res)
}

func blogPostFindComments(logger *slogger.Logger, coll *mongo.Collection, goctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	var res []interface{}
	cur, err := coll.Find(goctx, bson.D{{"post_id", bson.D{{"$gt", rand.Intn(90)}}}})
	if err != nil {
		return err
	}
	return cur.All(goctx, &res)
}

func blogPostUpdatePost(logger *slogger.Logger, coll *mongo.Collection, goctx context.Context) error {
	rand.Seed(time.Now().UnixNano())
	_, err := coll.UpdateOne(goctx, bson.D{{"_id", bson.D{{"$gt", rand.Intn(90)}}}}, bson.D{{"$inc", bson.D{{"upVotes", 1}}}})
	if err != nil {
		return fmt.Errorf("failed to update the doc. err=%v", err)
	}
	return nil
}

func runBlogApp(logger *slogger.Logger, client *mongo.Client, workerNum int, ctx context.Context) (time.Duration, bool, error) {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()

	postsColl := client.Database(BlogsDB).Collection(PostsColl)
	commentsColl := client.Database(BlogsDB).Collection(CommentsColl)

	funcs := []func(logger *slogger.Logger, coll *mongo.Collection, goctx context.Context) error{
		blogPostFindComments,
		blogPostFindOne,
		blogPostFindSome,
		blogPostUpdatePost,
	}

	ix := rand.Intn(4)
	var coll *mongo.Collection
	if ix == 0 {
		coll = commentsColl
	} else {
		coll = postsColl
	}
	if err := funcs[ix](logger, coll, ctx); err != nil {
		return 0, false, err
	}

	elapsed := time.Since(start)
	logger.Logf(slogger.DEBUG, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, true, nil
}

func insertBlogsData(posts, commentsPerPost int, client *mongo.Client, ctx context.Context) error {
	postsColl := client.Database(BlogsDB).Collection(PostsColl)
	commentsColl := client.Database(BlogsDB).Collection(CommentsColl)
	// insert some docs
	blogs := make([]interface{}, posts)
	comments := make([]interface{}, commentsPerPost*posts)
	k := 0
	for i := 0; i < posts; i++ {
		blogs[i] = getPostDoc(i)
		for j := 0; j < commentsPerPost; j++ {
			comments[k] = getCommentDoc(i, j)
			k++
		}
	}
	for i, v := range comments {
		if v == nil {
			return fmt.Errorf("%v is nil", i)

		}
	}
	if _, err := postsColl.InsertMany(ctx, blogs); err != nil {
		return fmt.Errorf("initial insert of posts failed. err: %v", err)
	}
	if _, err := commentsColl.InsertMany(ctx, comments); err != nil {
		return fmt.Errorf("initial insert of comments failed. err: %v", err)
	}
	return nil

}

func cleanupBlogApp(client *mongo.Client, ctx context.Context) error {
	return client.Database(BlogsDB).Drop(ctx)
}

func runProxyConnectionPerformanceBlogApp(iterations, mongoPort, proxyPort int, hostname string, logger *slogger.Logger, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, mode util.MongoConnectionMode, mongoClientFactory func(host string, port int, mode util.MongoConnectionMode, secondaryReads bool, appName string) (*mongo.Client, error)) error {
	preSetupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		return util.DisableFailPoint(client, ctx)
	}
	setupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		if err := cleanupBlogApp(client, ctx); err != nil {
			return err
		}
		return insertBlogsData(1000, 5, client, ctx)
	}

	testFunc := func(logger *slogger.Logger, client *mongo.Client, workerNum, iteration int, ctx context.Context) (elapsed time.Duration, success bool, err error) {
		return runBlogApp(logger, client, workerNum, ctx)
	}

	cleanupFunc := func(logger *slogger.Logger, client *mongo.Client, ctx context.Context) error {
		return cleanupBlogApp(client, ctx)
	}
	results, failedCount, maxLatencyMs, avgLatencyMs, percentiles, err := DoConcurrencyTestRun(logger,
		hostname, mongoPort, proxyPort, mode,
		mongoClientFactory,
		iterations, workers,
		preSetupFunc,
		setupFunc,
		testFunc,
		cleanupFunc,
	)

	return analyzeResults(err, workers, failedCount, avgLatencyMs, targetAvgLatencyMs, maxLatencyMs, targetMaxLatencyMs, results, percentiles, logger)
}
