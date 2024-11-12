# Env Lab API Server

研究室で運用している計測データを返すAPIサーバのソースです。

IoT機器で収集したデータを分析のためにAPIを介して取得できるようにしています。

> [!WARNING]
> 現在は運用していません。

## 機能
- 指定したセンサのデータを取得できる
- 日時を指定できるほか，最新の24時間のデータを返すこともできる
- 日の平均値や積算値を取得することができる

## クエリ例

### 多機能環境センサの最新24時間のデータを取得する

```
curl -X 'GET' \
  'https://api-dev.env.cs.i.nagoya-u.ac.jp/api/v2/latest_24h?data=multi_env_sensor&format=csv' \
  -H 'accept: application/json'
```

### 指定した期間の平均気温の推移を取得する

```
curl -X 'GET' \
  'https://api.env.cs.i.nagoya-u.ac.jp/api/v2/mean_temp?data=meter_zl6&format=csv&start_date=2023-09-01&end_date=2024-01-31' \
  -H 'accept: text/csv'
```

## その他

詳細はこちら: [https://iot.env.cs.i.nagoya-u.ac.jp/api_info/](https://iot.env.cs.i.nagoya-u.ac.jp/api_info/)

API Document: [https://api.env.cs.i.nagoya-u.ac.jp/docs#/](https://api.env.cs.i.nagoya-u.ac.jp/docs#/)

Powered by [FastAPI](https://fastapi.tiangolo.com/ja/)
