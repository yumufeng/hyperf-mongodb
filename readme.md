# hyperf mongodb pool

```
composer require yumufeng/hyperf-mongodb
```

## config 
在/config/autoload目录里面创建文件 mongodb.php
添加以下内容
```php
return [
    'default' => [
             'username' => env('MONGODB_USERNAME', ''),
             'password' => env('MONGODB_PASSWORD', ''),
             'host' => env('MONGODB_HOST', '127.0.0.1'),
             'port' => env('MONGODB_PORT', 27017),
             'db' => env('MONGODB_DB', 'test'),
             'authMechanism' => 'SCRAM-SHA-256',
             //设置复制集,没有不设置
             //'replica' => 'rs0',
            'pool' => [
                'min_connections' => 3,
                'max_connections' => 1000,
                'connect_timeout' => 10.0,
                'wait_timeout' => 3.0,
                'heartbeat' => -1,
                'max_idle_time' => (float) env('MONGODB_MAX_IDLE_TIME', 60),
            ],
    ],
];
```


# 使用案例

使用注解，自动加载 
**\Hyperf\Mongodb\MongoDb** 
```php
/**
 * @Inject()
 * @var MongoDb
*/
 protected $mongoDbClient;
```

#### **tips:** 
查询的值，是严格区分类型，string、int类型的哦

### 新增

单个添加
```php
$insert = [
            'account' => '',
            'password' => ''
];
$this->$mongoDbClient->insert('fans',$insert);
```

批量添加
```php
$insert = [
            [
                'account' => '',
                'password' => ''
            ],
            [
                'account' => '',
                'password' => ''
            ]
];
$this->$mongoDbClient->insertAll('fans',$insert);
```

### 查询

```php
$where = ['account'=>'1112313423'];
$result = $this->$mongoDbClient->fetchAll('fans', $where);
```

### 分页查询
```php
$list = $this->$mongoDbClient->fetchPagination('article', 10, 0, ['author' => $author]);
```

### 更新
```php
$where = ['account'=>'1112313423'];
$updateData = [];

$this->$mongoDbClient->updateColumn('fans', $where,$updateData); // 只更新数据满足$where的行的列信息中在$newObject中出现过的字段
$this->$mongoDbClient->updateRow('fans',$where,$updateData);// 更新数据满足$where的行的信息成$newObject
```
### 删除

```php
$where = ['account'=>'1112313423'];
$all = true; // 为false只删除匹配的一条，true删除多条
$this->$mongoDbClient->delete('fans',$where,$all);
```

### count统计

```php
$filter = ['isGroup' => "0", 'wechat' => '15584044700'];
$count = $this->$mongoDbClient->count('fans', $filter);
```



### Command，执行更复杂的mongo命令

**sql** 和 **mongodb** 关系对比图

|   SQL  | MongoDb |
| --- | --- |
|   WHERE  |  $match (match里面可以用and，or，以及逻辑判断，但是好像不能用where)  |
|   GROUP BY  | $group  |
|   HAVING  |  $match |
|   SELECT  |  $project  |
|   ORDER BY  |  $sort |
|   LIMIT  |  $limit |
|   SUM()  |  $sum |
|   COUNT()  |  $sum |

```php

$pipeline= [
            [
                '$match' => $where
            ], [
                '$group' => [
                    '_id' => [],
                    'groupCount' => [
                        '$sum' => '$groupCount'
                    ]
                ]
            ], [
                '$project' => [
                    'groupCount' => '$groupCount',
                    '_id' => 0
                ]
            ]
];

$count = $this->$mongoDbClient->command('fans', $pipeline);
```