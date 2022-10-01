db.createUser({
  user: "root",
  pwd: "root",
  roles: [
    {
      role: "readWrite",
      db: "efm-stations",
    },
  ],
});

db.createCollection("electric-fields");
