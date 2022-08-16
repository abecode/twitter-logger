# twitter-logger
Listens to tweets on ec2 with tweepy and archive them to s3 using logrotate, cron, and systemd.  Eventually also analyze the data...

For more info, see [this blog post](https://abecode.github.io/twitter/devops/linux/aws/2021/07/23/twitter-listener.html)

## Ingredients

- 1 ec2 instance (micro is fine) running stock aws linux
- 1 s3 bucket to store gzipped newline delimited json (ndjson)
- 1 twitter developer account with consumer key and secret and access key and secret

## Set up

- pull the repository to the ec2 instance
- export env variables, e.g.:
```{bash}
export TWITTER_USER="abestockmon"
export CONSUMER_KEY="fillinwithyourinfo"
export CONSUMER_SECRET="fillinwithyourinfo"
export ACCESS_TOKEN="fillinwithyourinfo"
export ACCESS_TOKEN_SECRET="fillinwithyourinfo"
```
- actually, you may want to just hard code these: I haven't tested it
  out yet, I just removed them and using env variables is a good
  practice but you may also need to modify the systemd service so that
  the env variables are set there.  Right now there is a placeholder,
  setup.env, which isn't checked in due to keeping the twitter
  credentials secure.
- install the logrotate.conf `logrotate logrotate.conf  -v --state logrotate.state`
- install the systemd service and run it
```{bash}
sudo cp hello.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart hello.service
```

# Analysis

[see this notebook](https://github.com/abecode/twitter-logger/blob/main/analysis/2022-01-18_snowflake_volume_analysis.ipynb)
