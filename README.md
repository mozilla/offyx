log ingestion endpoint compliant with [mozilla-services/Dockerflow](https://github.com/mozilla-services/Dockerflow)


if the body is not json, or does not evaluate to an object or list of objects, HTTP 400 will be returned

if the body is an array of objects then each object will generate a separate log

if the body is a nested object then it will be flattened using `.` delimited paths

if the body is an object then it will be logged to stdout in the mozlog `Fields` key
