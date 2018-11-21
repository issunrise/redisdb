package redisdb

import (
	// "flag"
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	cacheKey = "onlineClient"
	maxTask  = 1024
)

type RedisClient struct {
	pool *redis.Pool
	task chan *Task
}

type Task struct {
	Cmd  string
	Args redis.Args
}

func NewRedisClient(server, password, db string) *RedisClient {
	// log.Println("redis init:", server, password, db)
	return &RedisClient{pool: newPool(server, password, db)}
}

func ToRedisClient(pool *redis.Pool) *RedisClient {
	return &RedisClient{pool: pool}
}

func (c *RedisClient) NewPubSubConn() *redis.PubSubConn {
	return &redis.PubSubConn{c.pool.Get()}
}

func newPool(addr, auth, db string) *redis.Pool {
	pool := &redis.Pool{
		MaxIdle:     1,
		MaxActive:   100,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Println("err", err)
				return nil, err
			}

			if auth != "" {
				c.Do("auth", auth)
			}

			if db != "" {
				// log.Println("db", db)
				c.Do("select", db)
			}

			return c, nil
		},
	}

	// redisPoolMap[key] = pool

	return pool
}

func (c *RedisClient) InitTask(args ...int) *RedisClient {
	if len(args) > 0 {
		c.task = make(chan *Task, args[0])
		return c
	}
	c.task = make(chan *Task, maxTask)
	return c
}

func (c *RedisClient) GetTask() chan *Task {
	return c.task
}

func (c *RedisClient) Listen() {
	for {
		select {
		case in := <-c.task:
			c.Do(in)
		}
	}
}

// a.redisTaskCh <- &redisdb.Task{"publish", redis.Args{}.Add(key, data)
func (c *RedisClient) AddTask(in *Task) {
	c.task <- in
}

func (c *RedisClient) Do(q *Task) bool {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do(q.Cmd, q.Args...)
	if err != nil {
		log.Println(err, q.Cmd, q.Args)
		return false
	}
	return true
}

func (c *RedisClient) MapToArgs(key string, obj map[string]string) redis.Args {
	args := redis.Args{}.Add(key) // 转成数组
	for k, v := range obj {
		args = args.Add(k).Add(v)
	}
	return args
}

//获取所有在线用户
func (c *RedisClient) GetAll() {
	conn := c.pool.Get()
	defer conn.Close()
	clients, err := redis.StringMap(conn.Do("HGETALL", cacheKey))
	if err != nil {
		panic(err)
	}
	fmt.Printf("online client: %d \n", len(clients))
	for uId, client := range clients {
		fmt.Printf("%s -- %s\n", uId, client)
	}
}

//根据用户ID获取单个用户
func (c *RedisClient) GetOne(id string) {
	conn := c.pool.Get()
	defer conn.Close()
	client, err := redis.String(conn.Do("HGET", cacheKey, id))

	if err != nil {
		panic(err)
	}
	fmt.Println(client)
}

//踢出某个用户
func (c *RedisClient) Kick(id string) {
	conn := c.pool.Get()
	defer conn.Close()
	result, err := conn.Do("HDEL", cacheKey, id)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}

//清除所有在线用户信息
func (c *RedisClient) ClearAll() {
	conn := c.pool.Get()
	defer conn.Close()
	result, err := conn.Do("DEL", cacheKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}

//关闭redis连接池
func (c *RedisClient) Close() {
	if c.pool != nil {
		c.pool.Close()
	}
}

func (c *RedisClient) Publish(channelid, msg string) {
	conn := c.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channelid, msg)
	if err != nil {
		fmt.Println("redis PUBLISH error:", err)
		return
	}
}

func (c *RedisClient) Lrange(keyName string, start, stop int) []string {
	conn := c.pool.Get()
	defer conn.Close()
	list, err := redis.Strings(conn.Do("LRANGE", keyName, start, stop))
	if err != nil {
		log.Println("redis LRANGE error:", keyName, err)
		return nil
	}
	return list
}

func (c *RedisClient) LrangeByte(keyName string, start, stop int) [][]byte {
	conn := c.pool.Get()
	defer conn.Close()
	list, err := redis.ByteSlices(conn.Do("LRANGE", keyName, start, stop))
	if err != nil {
		log.Println("redis LRANGE error:", keyName, err)
		return nil
	}
	return list
}

func (c *RedisClient) LindexByte(keyName string, index int) []byte {
	conn := c.pool.Get()
	defer conn.Close()
	list, err := redis.Bytes(conn.Do("LINDEX", keyName, index))
	if err != nil {
		log.Println("redis LRANGE error:", keyName, err)
		return nil
	}
	return list
}

func (c *RedisClient) Hmset(keyName string, in interface{}) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := conn.Do("HMSET", redis.Args{}.Add(keyName).AddFlat(in)...)
	if err != nil {
		log.Println("redis HMSET error:", keyName, err)
		return nil
	}
	return v
}

func (c *RedisClient) HashToStruct(keyName string, in interface{}) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.Values(conn.Do("HGETALL", keyName))
	if err != nil {
		log.Println("redis HGETALL error:", keyName, err)
		return in
	}

	if err := redis.ScanStruct(v, in); err != nil {
		log.Println("redis HGETALL to struct error:", err)
		return in
	}

	return in
}

func (c *RedisClient) HashToMap(keyName string) map[string]string {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := redis.StringMap(conn.Do("HGETALL", keyName))
	if err != nil {
		log.Println("redis HGETALL to map error:", err)
		return nil
	}
	return v
}

func (c *RedisClient) Rpush(keyName, in string) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := conn.Do("RPUSH", keyName, in)
	if err != nil {
		log.Println("redis RPUSH error:", keyName, err)
		return nil
	}
	return v
}

func (c *RedisClient) Rpushs(keyName string, in []string) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := conn.Do("RPUSH", keyName, in)
	if err != nil {
		log.Println("redis RPUSH error:", keyName, err)
		return nil
	}
	return v
}
func (c *RedisClient) Lpop(keyName string) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := conn.Do("lpop", keyName)
	if err != nil {
		log.Println("redis RPUSH err:", keyName, err)
		return nil
	}
	return v
}

func (c *RedisClient) Llen(keyName string) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := conn.Do("llen", keyName)
	if err != nil {
		log.Println("redis RPUSH err:", keyName, err)
		return nil
	}
	return v
}

func (c *RedisClient) Ltrim(keyName string, start, end int) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := conn.Do("ltrim", keyName, start, end)
	if err != nil {
		log.Println("redis ltrim err:", keyName, err)
		return nil
	}
	return v
}

func (c *RedisClient) Keys(keyName string) interface{} {
	conn := c.pool.Get()
	defer conn.Close()
	v, err := conn.Do("keys", keyName)
	if err != nil {
		log.Println("redis RPUSH err:", keyName, err)
		return nil
	}
	return v
}

func (c *RedisClient) PSubscribe(do func(ch chan *redis.PMessage), channels ...interface{}) {
	ch := make(chan *redis.PMessage, 300)
	go do(ch)
	loop := func() {
		psc := c.NewPubSubConn()
		defer psc.Close()
		psc.PSubscribe(channels...)

		for {
			switch v := psc.Receive().(type) {
			case redis.PMessage:
				ch <- &v
			case redis.Subscription:
				log.Println("open redis sub:", v.Channel, v.Kind, v.Count)
			case error:
				log.Println("sub err: ", v)
				return
			}
		}
	}
	for {
		loop()
		time.Sleep(time.Second * 1)
	}
}

func (c *RedisClient) Subscribe(do func(ch chan *redis.Message), channels ...interface{}) {
	ch := make(chan *redis.Message, 300)
	go do(ch)
	loop := func() {
		psc := c.NewPubSubConn()
		defer psc.Close()
		psc.Subscribe(channels...)

		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				ch <- &v
			case redis.Subscription:
				log.Println("open redis sub:", v.Channel, v.Kind, v.Count)
			case error:
				log.Println("sub err: ", v)
				return
			}
		}
	}
	for {
		loop()
		time.Sleep(time.Second * 1)
	}
}
