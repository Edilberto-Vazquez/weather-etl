db.createUser({
    user: "root",
    pwd: "root",
    roles: [
        {
            role: "readWrite",
            db: "inaoe",
        },
    ],
});

db.createCollection("ElectricFields");
