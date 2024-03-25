CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_trusted` (
  `serialnumber` string,
  `sensorreadingtime` bigint,
  `distancefromobject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://danask-lake-house/stedi/step_trainer/trusted/'
TBLPROPERTIES ('classification' = 'json');