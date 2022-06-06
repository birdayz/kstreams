# Example

## Prepare topics

```shell
kaf topic create sensor-data -p 4 -b localhost:9092
kaf topic create message-count -p 4 -b localhost:9092

```

## Produce inputs

```shell
echo '{"temperature":5, "timestamp" : "2006-01-02T16:04:05Z"}' | kaf produce sensor-data --key device-b
```
