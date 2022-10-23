db.createUser({
  user: "root",
  pwd: "root",
  roles: [
    {
      role: "readWrite",
      db: "InaoeWeatherStations",
    },
  ],
});

db.createCollection("ElectricFields");
