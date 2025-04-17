truncate -s 100m file100mb
java -jar target/j3cli-1.0-SNAPSHOT.jar http://127.0.0.1:7070 us-east-1 test test mybucket ./file100mb 10485760
rm -f file100mb
