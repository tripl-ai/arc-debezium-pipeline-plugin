set -ex

until nc -z mongodb 27017
do
    sleep 1
done

mongo -u admin -p admin --authenticationDatabase admin mongodb:27017/inventory <<-EOF
    rs.initiate({
        _id: "rs0",
        members: [ { _id: 0, host: "mongodb:27017" } ]
    });
EOF
echo "Initiated replica set"

sleep 3

mongo -u admin -p admin --authenticationDatabase admin mongodb:27017/admin <<-EOF
    db.runCommand({
        createRole: "listDatabases",
        privileges: [
            { resource: { cluster : true }, actions: ["listDatabases"]}
        ],
        roles: []
    });

    db.createUser({
        user: 'debezium',
        pwd: 'dbz',
        roles: [
            { role: "readWrite", db: "inventory" },
            { role: "read", db: "local" },
            { role: "listDatabases", db: "admin" },
            { role: "read", db: "config" },
            { role: "read", db: "admin" }
        ]
    });
EOF

echo "Created users"
