package mgo

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"gitee.com/baixudong/bar"
	"gitee.com/baixudong/gson"
	"gitee.com/baixudong/kinds"
	"gitee.com/baixudong/requests"
	"gitee.com/baixudong/thread"
	"gitee.com/baixudong/tools"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var ErrNoDocuments = mongo.ErrNoDocuments

// mongodb 的操作========================================================================== start
type Client struct {
	client *mongo.Client
}

type FindOption struct {
	BatchSize       int32          // 服务器返回的每个批次中包含的最大文件数。
	Limit           int64          //要返回的最大文档数
	Timeout         time.Duration  //超时时间
	NoCursorTimeout bool           //操作所创建的游标在一段时间不活动后不会超时
	Show            map[string]int //描述哪些字段将被包含在操作返回的文件中的文件
	Skip            int64          //在将文档添加到结果中之前要跳过的文档数量
	Sort            map[string]int // 一个文件，指定返回文件的顺序
	Await           bool           //oplog 是否阻塞等待数据
}
type ClientOption struct {
	Host    string
	Port    int
	Usr     string
	Pwd     string
	Direct  bool
	HostMap map[string]string
}
type FindsData struct {
	cursor  *mongo.Cursor
	filter  any
	mongoOp *options.FindOptions
	object  *mongo.Collection
	raw     map[string]any
	rawOk   bool
}
type FindData struct {
	object *mongo.SingleResult
	raw    map[string]any
}
type UpateResult struct {
	MatchedCount  int64 // 匹配的个数
	ModifiedCount int64 // 文档变更的数量,不包括增加
	UpsertedCount int64 // upsert的数量
	UpsertedID    primitive.ObjectID
	Exists        bool //是否存在
}
type ObjectID = primitive.ObjectID
type Timestamp = primitive.Timestamp            //{T uint32   I uint32}
var ObjectIDFromHex = primitive.ObjectIDFromHex //十六进制字符串转objectId
var NewObjectID = primitive.NewObjectID         //创建一个新的objectid

func (obj *FindData) Map() map[string]any {
	if obj.raw == nil {
		raw := map[string]any{}
		obj.object.Decode(&raw)
		obj.raw = raw
		return raw
	}
	return obj.raw
}

// 使用json.Unmarshal 解码
func (obj *FindData) Decode(val any) (err error) {
	_, err = gson.Decode(obj.Map(), val)
	return
}

// 返回gjson
func (obj *FindData) Json() *gson.Client {
	result, _ := gson.Decode(obj.Map())
	return result
}

// 返回json
func (obj *FindData) String() string {
	return tools.BytesToString(obj.Bytes())
}

// 返回字节
func (obj *FindData) Bytes() []byte {
	con, _ := gson.Encode(obj.Map())
	return con
}

// 重试
func (obj *FindsData) ReTry(ctx context.Context) error {
	rs, err := obj.object.Find(ctx, obj.filter, obj.mongoOp)
	if err != nil {
		return err
	} else {
		obj.cursor = rs
	}
	return nil
}

// 是否有下一个数据
func (obj *FindsData) Next(ctx context.Context) bool {
	obj.rawOk = true
	rs := obj.cursor.Next(ctx)
	if !rs {
		obj.Close(ctx)
	}
	return rs
}

// 关闭游标
func (obj *FindsData) Close(ctx context.Context) error {
	return obj.cursor.Close(ctx)
}

// 返回游标的长度
func (obj *FindsData) Len() int {
	return obj.cursor.RemainingBatchLength()
}

// 返回gjson
func (obj *FindsData) Json() *gson.Client {
	result, _ := gson.Decode(obj.Map())
	return result
}

func (obj *FindsData) Map() map[string]any {
	if !obj.rawOk {
		return obj.raw
	}
	obj.rawOk = false
	raw := map[string]any{}
	obj.cursor.Decode(&raw)
	obj.raw = raw
	return raw
}

// 使用json.Unmarshal 解码
func (obj *FindsData) Decode(val any) (err error) {
	_, err = gson.Decode(obj.Map(), val)
	return
}

// 返回json
func (obj *FindsData) String() string {
	return tools.BytesToString(obj.Bytes())
}

// 返回字节
func (obj *FindsData) Bytes() []byte {
	con, _ := gson.Encode(obj.Map())
	return con
}

type mgoDialer struct {
	dialer  *net.Dialer
	hostMap map[string]string
}

func (obj *mgoDialer) DialContext(ctx context.Context, network string, addr string) (net.Conn, error) {
	if obj.hostMap != nil {
		host, port, err := net.SplitHostPort(addr)
		if err == nil {
			val, ok := obj.hostMap[host]
			if ok {
				addr = val + ":" + port
			}
		}
	}
	return obj.dialer.DialContext(ctx, network, addr)
}

// 新建客户端
func NewClient(ctx context.Context, opt ClientOption) (*Client, error) {
	if opt.Host == "" {
		opt.Host = "localhost"
	}
	if opt.Port == 0 {
		opt.Port = 27017
	}
	uri := fmt.Sprintf("mongodb://%s:%d", opt.Host, opt.Port)
	httpClient, err := requests.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	clientOption := &options.ClientOptions{
		HTTPClient: httpClient.HttpClient(),
		BSONOptions: &options.BSONOptions{
			UseJSONStructTags: true,
		},
	}
	clientOption.ApplyURI(uri)
	if opt.Usr != "" && opt.Pwd != "" {
		clientOption.SetAuth(options.Credential{
			Username: opt.Usr,
			Password: opt.Pwd,
		})
	}
	mgoDialer := &mgoDialer{hostMap: opt.HostMap}
	mgoDialer.dialer = requests.NewDail(ctx, requests.DialOption{}).Dialer()
	clientOption.SetDialer(mgoDialer)
	clientOption.SetDirect(opt.Direct)
	clientOption.SetDisableOCSPEndpointCheck(true)
	clientOption.SetRetryReads(true)
	clientOption.SetRetryWrites(true)

	client, err := mongo.Connect(ctx, clientOption)
	if err != nil {
		return nil, err
	}
	return &Client{client}, client.Ping(ctx, readpref.Primary())
}

type Db struct {
	db   *mongo.Database
	name string
}

// 集合
type Table struct {
	db     *Db
	table  *mongo.Collection
	dbName string
	name   string
}

// 创建新的集合
func (obj *Client) NewDb(dbName string) *Db {
	return &Db{db: obj.client.Database(dbName), name: dbName}
}
func (obj *Db) NewTable(tableName string) *Table {
	return &Table{db: obj, table: obj.db.Collection(tableName), dbName: obj.name, name: tableName}
}
func (obj *Db) Tables(ctx context.Context) ([]string, error) {
	return obj.db.ListCollectionNames(ctx, map[string]string{})
}

// 创建新的集合
func (obj *Client) NewTable(dbName string, tableName string) *Table {
	return obj.NewDb(dbName).NewTable(tableName)
}

// 创建新的集合
func (obj *Table) NewTable(tableName string) *Table {
	return obj.db.NewTable(tableName)
}
func (obj *Client) Dbs(ctx context.Context) ([]string, error) {
	return obj.client.ListDatabaseNames(ctx, map[string]string{})
}

func (obj *Table) DbName() string {
	return obj.dbName
}

func (obj *Db) Name() string {
	return obj.name
}

// 集合名称
func (obj *Table) Name() string {
	return obj.name
}

// findone
func (obj *Table) Find(pre_ctx context.Context, filter any, opts ...FindOption) (*FindData, error) {
	opt := FindOption{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	if filter == nil {
		filter = map[string]string{}
	}
	mongo_op := options.FindOneOptions{
		Projection:      opt.Show,
		Skip:            &opt.Skip,
		Sort:            opt.Sort,
		NoCursorTimeout: &opt.NoCursorTimeout,
	}
	if opt.Timeout != 0 {
		tot := opt.Timeout
		mongo_op.MaxTime = &tot
	}
	rs := obj.table.FindOne(pre_ctx, filter, &mongo_op)
	if rs.Err() == ErrNoDocuments {
		return nil, nil
	}
	return &FindData{
		object: rs,
	}, rs.Err()
}

// 判断数据是否存在
func (obj *Table) Exist(pre_ctx context.Context, filter any) (bool, error) {
	rs, err := obj.Find(pre_ctx, filter, FindOption{
		Show: map[string]int{"_id": 1},
	})
	if err != nil {
		return false, err
	}
	if rs == nil {
		return false, nil
	} else {
		return true, nil
	}
}

// findmany
func (obj *Table) Finds(pre_ctx context.Context, filter any, opts ...FindOption) (*FindsData, error) {
	opt := FindOption{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	if filter == nil {
		filter = map[string]string{}
	}
	if opt.BatchSize == 0 {
		opt.BatchSize = 50
	}
	mongo_op := options.FindOptions{
		Projection:      opt.Show,
		Skip:            &opt.Skip,
		Sort:            opt.Sort,
		NoCursorTimeout: &opt.NoCursorTimeout,
	}
	if opt.Await {
		tailValue := options.TailableAwait
		mongo_op.CursorType = &tailValue
	}
	if opt.Limit != 0 {
		mongo_op.Limit = &opt.Limit
	}
	if opt.BatchSize != 0 {
		mongo_op.BatchSize = &opt.BatchSize
	}
	if opt.Timeout != 0 {
		tot := opt.Timeout
		mongo_op.MaxTime = &tot
	}
	rs, err := obj.table.Find(pre_ctx, filter, &mongo_op)
	return &FindsData{cursor: rs, filter: filter, mongoOp: &mongo_op, object: obj.table}, err
}

// 集合数量
func (obj *Table) Count(pre_ctx context.Context, filter any, opts ...FindOption) (int64, error) {
	opt := FindOption{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	if filter == nil {
		mongo_op := options.EstimatedDocumentCountOptions{}
		if opt.Timeout != 0 {
			tot := opt.Timeout
			mongo_op.MaxTime = &tot
		}
		return obj.table.EstimatedDocumentCount(pre_ctx, &mongo_op)
	}
	mongo_op := options.CountOptions{}
	if opt.Timeout != 0 {
		tot := opt.Timeout
		mongo_op.MaxTime = &tot
	}
	if opt.Limit != 0 {
		mongo_op.Limit = &opt.Limit
	}
	if opt.Skip != 0 {
		mongo_op.Skip = &opt.Skip
	}
	return obj.table.CountDocuments(pre_ctx, filter, &mongo_op)
}

// 添加文档
func (obj *Table) Add(pre_ctx context.Context, document any) (primitive.ObjectID, error) {
	var rs_id primitive.ObjectID
	res, err := obj.table.InsertOne(pre_ctx, document)
	if err != nil {
		return rs_id, err
	}
	rs_id = res.InsertedID.(primitive.ObjectID)
	return rs_id, err
}

// 添加一批文档
func (obj *Table) Adds(pre_ctx context.Context, document ...any) ([]primitive.ObjectID, error) {
	rs_ids := []primitive.ObjectID{}
	document_len := len(document)
	if document_len == 0 {
		return rs_ids, nil
	}
	res, err := obj.table.InsertMany(pre_ctx, document)
	if err != nil {
		return rs_ids, err
	}
	for _, insert_id := range res.InsertedIDs {
		rs_ids = append(rs_ids, insert_id.(primitive.ObjectID))
	}
	return rs_ids, err
}

// 删除一个文档
func (obj *Table) Del(pre_ctx context.Context, document any) (int64, error) {
	res, err := obj.table.DeleteOne(pre_ctx, document)
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, err
}

// 删除一些文档
func (obj *Table) Dels(pre_ctx context.Context, document any) (int64, error) {
	res, err := obj.table.DeleteMany(pre_ctx, document)
	if err != nil {
		return 0, err
	}
	return res.DeletedCount, err
}

// 更新一个文档
func (obj *Table) Update(pre_ctx context.Context, filter any, update any, values ...map[string]any) (UpateResult, error) {
	var result UpateResult
	updateData := map[string]any{}
	if update != nil {
		updateData["$set"] = update
	}
	for _, value := range values {
		for kk, vv := range value {
			updateData[kk] = vv
		}
	}

	res, err := obj.table.UpdateOne(pre_ctx, filter, updateData)
	if err != nil {
		return result, err
	}
	result.MatchedCount = res.MatchedCount
	result.ModifiedCount = res.ModifiedCount
	result.UpsertedCount = res.UpsertedCount
	return result, err
}

// 更新一些文档
func (obj *Table) Updates(pre_ctx context.Context, filter any, update any, values ...map[string]any) (UpateResult, error) {
	var result UpateResult
	updateData := map[string]any{}
	if update != nil {
		updateData["$set"] = update
	}
	for _, value := range values {
		for kk, vv := range value {
			updateData[kk] = vv
		}
	}
	res, err := obj.table.UpdateMany(pre_ctx, filter, updateData)
	if err != nil {
		return result, err
	}
	result.MatchedCount = res.MatchedCount
	result.ModifiedCount = res.ModifiedCount
	result.UpsertedCount = res.UpsertedCount
	return result, err
}

// upsert 一个文档
func (obj *Table) Upsert(pre_ctx context.Context, filter any, update any, values ...map[string]any) (UpateResult, error) {
	var result UpateResult
	if update == nil {
		update = map[string]string{}
	}
	updateData := map[string]any{}
	if update != nil {
		updateData["$set"] = update
	}
	for _, value := range values {
		for kk, vv := range value {
			updateData[kk] = vv
		}
	}

	c := true
	res, err := obj.table.UpdateOne(pre_ctx, filter, updateData, &options.UpdateOptions{Upsert: &c})
	if err != nil {
		return result, err
	}
	result.MatchedCount = res.MatchedCount
	result.ModifiedCount = res.ModifiedCount
	result.UpsertedCount = res.UpsertedCount
	if res.UpsertedID != nil {
		result.UpsertedID = res.UpsertedID.(primitive.ObjectID)
	}
	if result.MatchedCount > 0 {
		result.Exists = true
	}
	return result, err
}

// upsert 一些文档
func (obj *Table) Upserts(pre_ctx context.Context, filter any, update any, values ...map[string]any) (UpateResult, error) {
	var result UpateResult
	if update == nil {
		update = map[string]string{}
	}
	updateData := map[string]any{}
	if update != nil {
		updateData["$set"] = update
	}
	for _, value := range values {
		for kk, vv := range value {
			updateData[kk] = vv
		}
	}
	c := true
	res, err := obj.table.UpdateMany(pre_ctx, filter, updateData, &options.UpdateOptions{Upsert: &c})
	if err != nil {
		return result, err
	}
	result.MatchedCount = res.MatchedCount
	result.ModifiedCount = res.ModifiedCount
	result.UpsertedCount = res.UpsertedCount
	if res.UpsertedID != nil {
		result.UpsertedID = res.UpsertedID.(primitive.ObjectID)
	}
	if result.MatchedCount > 0 {
		result.Exists = true
	}
	return result, err
}

type ClearOption struct {
	Thread         int64          //线程数量
	Init           bool           //是否初始化
	Oid            ObjectID       //起始id
	Show           map[string]int //展示的字段
	Desc           bool           //是否倒序
	Filter         map[string]any //查询参数
	Bar            bool           //是否开启进度条
	BatchSize      int32          //服务器每批次多少
	ClearBatchSize int64          //每次清洗的批次
	Debug          bool           //是否开启debug
}
type ClearOplogOption struct {
	Thread    int64          //线程数量
	Init      bool           //是否初始化
	Oid       Timestamp      //起始id
	Show      map[string]int //展示的字段
	Filter    map[string]any //查询参数
	BatchSize int32          //服务器每批次多少
	Debug     bool           //是否开启debug
}

// 清洗oplog集合
func (obj *Client) ClearOplog(preCctx context.Context, Func func(context.Context, Oplog, ObjectID, Timestamp) (ObjectID, Timestamp, error), tag string, clearOptions ...ClearOplogOption) error {
	pre_ctx, pre_cnl := context.WithCancel(preCctx)
	defer pre_cnl()
	syncFilter := map[string]string{
		"tableName": "oplog.rs",
		"tag":       tag,
	}
	var clearOption ClearOplogOption
	if len(clearOptions) > 0 {
		clearOption = clearOptions[0]
	}
	if clearOption.Filter == nil {
		clearOption.Filter = map[string]any{}
	}
	if clearOption.Thread == 0 {
		clearOption.Thread = 100
	}
	if clearOption.Oid.IsZero() && !clearOption.Init {
		syncData, err := obj.NewTable("oplogSyncDataFile", "TempSyncData").Find(pre_ctx, syncFilter)
		if err != nil {
			return err
		}
		if syncData != nil {
			clearOption.Oid = syncData.Map()["oid"].(Timestamp)
		}
	}
	obj.NewTable("oplogSyncDataFile", "TempSyncData").Upsert(pre_ctx, syncFilter, map[string]Timestamp{"oid": clearOption.Oid})
	var datas *FindsData
	var err error
	if !clearOption.Oid.IsZero() {
		clearOption.Filter["ts"] = map[string]Timestamp{"$gte": clearOption.Oid}
	}
	datas, err = obj.NewTable("local", "oplog.rs").Finds(pre_ctx, clearOption.Filter, FindOption{Show: clearOption.Show, Await: true, BatchSize: clearOption.BatchSize})
	if err != nil {
		return err
	}
	defer datas.Close(pre_ctx)

	var cur int64
	var lastOid Timestamp
	taskMap := kinds.NewSet[ObjectID]()
	pool := thread.NewClient(pre_ctx, clearOption.Thread, thread.ClientOption{
		Debug: clearOption.Debug,
		TaskDoneCallBack: func(t *thread.Task) error {
			cur++
			if t.Error != nil {
				return t.Error
			}
			if t.Result[2] != nil {
				return t.Result[2].(error)
			}
			taskMap.Del(t.Result[0].(ObjectID))
			lastOid = t.Result[1].(Timestamp)
			if cur%int64(clearOption.Thread) == 0 {
				if _, err := obj.NewTable("oplogSyncDataFile", "TempSyncData").Upsert(pre_ctx, syncFilter, map[string]Timestamp{"oid": lastOid}); err != nil {
					return nil
				}
			}
			return nil
		},
	})
	defer pool.Close()
	var afterTime *time.Timer
	defer func() {
		if afterTime != nil {
			afterTime.Stop()
		}
	}()
	for datas.Next(pre_ctx) {
		data := ClearOplog(datas.Map())
		if data.ObjectID.IsZero() {
			continue
		}
		for taskMap.Has(data.ObjectID) {
			if afterTime == nil {
				afterTime = time.NewTimer(time.Second)
			} else {
				afterTime.Reset(time.Second)
			}
			select {
			case <-pool.Done():
				return pool.Err()
			case <-afterTime.C:
			}
		}
		_, err := pool.Write(&thread.Task{
			Func: Func, Args: []any{data, data.ObjectID, data.Timestamp},
		})
		if err != nil {
			return err
		}
	}
	if err := pool.Join(); err != nil {
		return err
	}
	if !lastOid.IsZero() {
		if _, err := obj.NewTable("oplogSyncDataFile", "TempSyncData").Upsert(pre_ctx, syncFilter, map[string]Timestamp{"oid": lastOid}); err != nil {
			return err
		}
	}
	return nil
}

// 清洗集合数据
func (obj *Table) clearTable(preCtx context.Context, Func any, tag string, clearOption ClearOption) error {
	if preCtx == nil {
		preCtx = context.TODO()
	}
	pre_ctx, pre_cnl := context.WithCancel(preCtx)
	defer pre_cnl()
	syncFilter := map[string]string{
		"tableName": obj.Name(),
		"tag":       tag,
	}
	if clearOption.Filter == nil {
		clearOption.Filter = map[string]any{}
	}
	if clearOption.Thread == 0 {
		clearOption.Thread = 100
	}
	var barTotal int64
	var barCur int64
	var TotalOk bool
	var CurOk bool
	var curTitle string
	if clearOption.Desc {
		curTitle = "descCount"
		obj.NewTable("TempSyncData").Update(pre_ctx, syncFilter, nil, map[string]any{
			"$unset": map[string]any{"ascCount": ""},
		})
	} else {
		curTitle = "ascCount"
		obj.NewTable("TempSyncData").Update(pre_ctx, syncFilter, nil, map[string]any{
			"$unset": map[string]any{"descCount": ""},
		})
	}
	if clearOption.Oid.IsZero() && !clearOption.Init {
		syncData, err := obj.NewTable("TempSyncData").Find(pre_ctx, syncFilter)
		if err != nil {
			return err
		}
		if syncData != nil {
			clearOption.Oid = syncData.Map()["oid"].(ObjectID)
			var CurAny any
			var TotalAny any
			if clearOption.Desc {
				CurAny, CurOk = syncData.Map()[curTitle]
			} else {
				CurAny, CurOk = syncData.Map()[curTitle]
			}
			if CurOk {
				barCur = CurAny.(int64)
			}
			TotalAny, TotalOk = syncData.Map()["total"]
			if TotalOk {
				barTotal = TotalAny.(int64)
			}
		}
	}
	lgte := "$gte"
	lgteInt := 1
	if clearOption.Desc {
		lgte = "$lte"
		lgteInt = -1
	}
	if !clearOption.Oid.IsZero() {
		clearOption.Filter["_id"] = map[string]ObjectID{lgte: clearOption.Oid}
	}

	var datas *FindsData
	var err error

	datas, err = obj.Finds(pre_ctx, clearOption.Filter, FindOption{Sort: map[string]int{"_id": lgteInt}, Show: clearOption.Show})
	if err != nil {
		return err
	}
	defer datas.Close(pre_ctx)
	if !TotalOk || !clearOption.Desc || clearOption.Oid.IsZero() {
		barTotal, err = obj.Count(pre_ctx, nil)
		if err != nil {
			return err
		}
	}
	_, err = obj.NewTable("TempSyncData").Upsert(pre_ctx, syncFilter, map[string]any{"oid": clearOption.Oid, "total": barTotal})
	if err != nil {
		return nil
	}
	if clearOption.Oid.IsZero() {
		barCur = 0
	}
	var lastOid ObjectID
	bar := bar.NewClient(barTotal, bar.ClientOption{Cur: barCur})
	pool := thread.NewClient(pre_ctx, clearOption.Thread, thread.ClientOption{
		Debug: clearOption.Debug,
		TaskDoneCallBack: func(t *thread.Task) error {
			if t.Error != nil {
				return t.Error
			}
			if t.Result[1] != nil {
				return t.Result[1].(error)
			}
			barCur++
			if clearOption.Bar {
				bar.Print()
			}
			lastOid = t.Result[0].(ObjectID)
			if barCur%int64(clearOption.Thread) == 0 {
				if _, err := obj.NewTable("TempSyncData").Upsert(pre_ctx, syncFilter, map[string]any{"oid": lastOid, curTitle: barCur}); err != nil {
					return err
				}
			}
			return nil
		},
	})
	defer pool.Close()

	var tmId ObjectID
	if clearOption.ClearBatchSize > 0 {
		tempDatas := []map[string]any{}
		for datas.Next(pre_ctx) {
			data := datas.Map()
			tmId = data["_id"].(ObjectID)
			tempDatas = append(tempDatas, data)
			if len(tempDatas) >= int(clearOption.ClearBatchSize) {
				_, err := pool.Write(&thread.Task{
					Func: Func,
					Args: []any{tempDatas, tmId},
				})
				if err != nil {
					return err
				}
				tempDatas = []map[string]any{}
			}
		}
		tempDatasLen := len(tempDatas)
		if tempDatasLen > 0 {
			if _, err := pool.Write(&thread.Task{
				Func: Func,
				Args: []any{tempDatas, tmId},
			}); err != nil {
				return err
			}
		}
	} else {
		for datas.Next(pre_ctx) {
			data := datas.Map()
			tmId = data["_id"].(ObjectID)
			_, err := pool.Write(&thread.Task{
				Func: Func,
				Args: []any{data, tmId},
			})
			if err != nil {
				return err
			}
		}
	}
	if err := pool.Join(); err != nil {
		return err
	}
	if !lastOid.IsZero() {
		if _, err := obj.NewTable("TempSyncData").Upsert(pre_ctx, syncFilter, map[string]any{"oid": lastOid, curTitle: barCur}); err != nil {
			return err
		}
	}
	return nil
}

// 清洗集合数据
func (obj *Table) ClearTable(preCtx context.Context, Func func(context.Context, map[string]any, ObjectID) (ObjectID, error), tag string, clearOptions ...ClearOption) error {
	var clearOption ClearOption
	if len(clearOptions) > 0 {
		clearOption = clearOptions[0]
	}
	return obj.clearTable(preCtx, Func, tag, clearOption)
}

// 批量清洗集合数据
func (obj *Table) ClearTables(preCtx context.Context, Func func(context.Context, []map[string]any, ObjectID) (ObjectID, error), tag string, clearOptions ...ClearOption) error {
	var clearOption ClearOption
	if len(clearOptions) > 0 {
		clearOption = clearOptions[0]
	}
	if clearOption.ClearBatchSize <= 0 {
		clearOption.ClearBatchSize = 100
	}
	return obj.clearTable(preCtx, Func, tag, clearOption)
}

type Oplog struct {
	Op        string    //操作
	Ns        string    //表名
	Timestamp Timestamp //操作时间
	ObjectID  ObjectID
	Data      map[string]any //数据
}

// 清洗oplog data
func ClearOplog(oplogData map[string]any) Oplog {
	var result Oplog
	op := oplogData["op"].(string)    //操作
	ns := oplogData["ns"].(string)    // 表名
	ts := oplogData["ts"].(Timestamp) //时间
	result.Op = op
	result.Ns = ns
	result.Timestamp = ts
	if strings.HasPrefix(ns, "config.") {
		return result
	}
	var mond map[string]any
	var hid ObjectID
	oAny := oplogData["o"].(map[string]any)
	if op == "u" {
		oAny2 := oplogData["o2"].(map[string]any)
		hidAny, ok := oAny["_id"]
		if ok {
			switch val := hidAny.(type) {
			case ObjectID:
				hid = val
			default:
				// log.Print(gson.Decode(oplogData).Raw)
				// panic("未知的数据类型")
			}
		} else {
			switch val := oAny2["_id"].(type) {
			case ObjectID:
				hid = val
			default:
				// log.Print(gson.Decode(oplogData).Raw)
				// panic("未知的数据类型2")
			}
		}
		mondAny, ok := oAny["$set"]
		mondAny2, ok2 := oAny2["$set"]
		if ok {
			mond = mondAny.(map[string]any)
		} else if ok2 {
			mond = mondAny2.(map[string]any)
		} else {
			if len(oAny2) > len(oAny) {
				mond = oAny2
			} else {
				mond = oAny
			}
		}
	} else {
		switch val := oAny["_id"].(type) {
		case ObjectID:
			hid = val
		default:
			// log.Print(gson.Decode(oplogData).Raw)
			// panic("未知的数据类型3")
		}
	}
	result.Data = mond
	result.ObjectID = hid
	return result
}
