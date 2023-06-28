# https://www.postgresqltutorial.com/postgresql-delete/
DELETE FROM links
WHERE id = 7
RETURNING *;
