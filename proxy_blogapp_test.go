package mongonet

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const BlogsDB = "blogs"
const PostsColl = "posts"
const CommentsColl = "comments"

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

func getRandStringArray(maxArrLen, strLen int) []string {
	arrLen := rand.Intn(maxArrLen) + 1
	arr := make([]string, arrLen)
	for i := 0; i < arrLen; i++ {
		arr[i] = RandString(strLen)
	}
	return arr
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

func runBlogApp(logger *slogger.Logger, host string, proxyPort, workerNum int, mode MongoConnectionMode) (time.Duration, bool, error) {
	rand.Seed(time.Now().UnixNano())
	start := time.Now()
	client, err := getTestClient(host, proxyPort, mode, false, fmt.Sprintf("worker-%v", workerNum))
	if err != nil {
		return 0, false, fmt.Errorf("failed to get a test client. err=%v", err)
	}
	goctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSec)
	defer cancelFunc()
	if err := client.Connect(goctx); err != nil {
		return 0, false, fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(goctx)

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
	if err := funcs[ix](logger, coll, goctx); err != nil {
		return 0, false, err
	}

	elapsed := time.Since(start)
	logger.Logf(slogger.DEBUG, "worker-%v finished after %v", workerNum, elapsed)
	return elapsed, true, nil
}

func insertBlogsData(posts, commentsPerPost int, hostname string, port int, mode MongoConnectionMode) error {
	client, err := getTestClient(hostname, port, mode, false, "insertBlogsData")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSec)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}

	defer client.Disconnect(ctx)

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
	// fmt.Println(comments)
	if _, err := postsColl.InsertMany(ctx, blogs); err != nil {
		return fmt.Errorf("initial insert of posts failed. err: %v", err)
	}
	if _, err := commentsColl.InsertMany(ctx, comments); err != nil {
		return fmt.Errorf("initial insert of comments failed. err: %v", err)
	}
	return nil

}

func cleanupBlogApp(host string, proxyPort int, mode MongoConnectionMode) error {
	client, err := getTestClient(host, proxyPort, mode, false, "cleanup")
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), ClientTimeoutSec)
	defer cancelFunc()
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to server. err: %v", err)
	}
	defer client.Disconnect(ctx)

	return client.Database(BlogsDB).Drop(ctx)
}

func privateConnectionPerformanceTesterBlogApp(mode MongoConnectionMode, maxPoolSize, workers int, targetAvgLatencyMs, targetMaxLatencyMs int64, t *testing.T) {
	Iterations := 20
	mongoPort, proxyPort, hostname := getHostAndPorts()
	t.Logf("using proxy port=%v, pool size=%v", proxyPort, maxPoolSize)
	hostToUse := hostname
	if mode == Direct {
		hostToUse = "localhost"
	}
	serverPort := proxyPort
	preSetupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		if err := cleanupBlogApp(hostname, serverPort, mode); err != nil {
			return err
		}
		return disableFailPoint(hostname, mongoPort, mode)
	}
	setupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return insertBlogsData(1000, 5, hostname, serverPort, mode)
	}

	testFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort, workerNum, iteration int, mode MongoConnectionMode) (elapsed time.Duration, success bool, err error) {
		return runBlogApp(logger, hostname, serverPort, workerNum, mode)
	}

	cleanupFunc := func(logger *slogger.Logger, hostname string, mongoPort, proxyPort int, mode MongoConnectionMode) error {
		return cleanupBlogApp(hostname, serverPort, mode)
	}

	pc := getProxyConfig(hostToUse, mongoPort, proxyPort, maxPoolSize, DefaultMaxPoolIdleTimeSec, mode, false)
	pc.LogLevel = slogger.DEBUG
	proxy, err := NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	go proxy.Run()

	results, failedCount, maxLatencyMs, avgLatencyMs, percentiles, err := DoConcurrencyTestRun(proxy.NewLogger("tester"),
		hostToUse, mongoPort, proxyPort, mode,
		Iterations, workers,
		preSetupFunc,
		setupFunc,
		testFunc,
		cleanupFunc,
	)

	if failedCount > 0 {
		t.Errorf("failed workers %v", failedCount)
	}
	if avgLatencyMs > targetAvgLatencyMs {
		t.Errorf("average latency %v > %v", avgLatencyMs, targetAvgLatencyMs)
	}
	if maxLatencyMs > targetMaxLatencyMs {
		t.Errorf("max latency %v > %v", maxLatencyMs, targetMaxLatencyMs)
	}
	if len(results) == 0 {
		t.Errorf("no successful runs!")
	}
	t.Logf("ALL DONE workers=%v, successful runs=%v, avg=%vms, max=%vms, failures=%v, percentiles=%v\nresults=%v", workers, len(results), avgLatencyMs, maxLatencyMs, failedCount, percentiles, results)
}

func TestProxyMongosModeConnectionPerformanceBlogAppFiveThreads(t *testing.T) {
	privateConnectionPerformanceTesterBlogApp(Cluster, 0, 5, 650, 1200, t)
}

func TestProxyMongosModeConnectionPerformanceBlogAppTwentyThreads(t *testing.T) {
	privateConnectionPerformanceTesterBlogApp(Cluster, 0, 20, 100, 500, t)
}

func TestProxyMongosModeConnectionPerformanceBlogAppSixtyThreads(t *testing.T) {
	privateConnectionPerformanceTesterBlogApp(Cluster, 0, 60, 200, 1500, t)
}

func TestProxyMongodModeConnectionPerformanceBlogAppFiveThreads(t *testing.T) {
	privateConnectionPerformanceTesterBlogApp(Direct, 0, 5, 650, 1200, t)
}

func TestProxyMongodModeConnectionPerformanceBlogAppTwentyThreads(t *testing.T) {
	privateConnectionPerformanceTesterBlogApp(Direct, 0, 20, 100, 500, t)
}

func TestProxyMongodModeConnectionPerformanceBlogAppSixtyThreads(t *testing.T) {
	privateConnectionPerformanceTesterBlogApp(Direct, 0, 60, 200, 1500, t)
}
