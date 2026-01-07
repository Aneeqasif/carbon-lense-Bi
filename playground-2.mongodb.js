db.grantRolesToUser("admin",
   [
  { role: "readWrite", db: "carbonLens" },
  { role: "dbAdmin", db: "carbonLens" }
]);