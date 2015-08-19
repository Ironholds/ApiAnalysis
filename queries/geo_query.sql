ADD JAR /srv/deployment/analytics/refinery/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION search_type AS 'org.wikimedia.analytics.refinery.hive.SearchClassifierUDF';
USE wmf;
SELECT
dt AS timestamp,
geocoded_data['country_code'] AS country,
user_agent,
user_agent_map['browser_family'] AS browser,
user_agent_map['browser_major'] AS browser_version,
uri_query AS query
FROM webrequest;
WHERE year = 2015
AND month = 08
AND ((day = 03 AND hour = 07) OR (day = 06 AND hour = 14) OR (day = 12 AND hour = 22) OR (day = 16 AND hour = 04))
AND search_type(uri_path, uri_query) = 'geo'
AND webrequest_source IN('text','mobile');
