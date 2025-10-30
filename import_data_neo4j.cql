// --- Xóa toàn bộ dữ liệu cũ ---
MATCH (n) DETACH DELETE n;

// --- Import Person nodes ---
LOAD CSV WITH HEADERS FROM 'file:///Person.csv' AS row
MERGE (p:Person {name: row.name})
SET p.id = row.id,
    p.family_name = row.family_name,
    p.gender = row.gender,
    p.born_on_date = row.born_on_date,
    p.died_on_date = row.died_on_date,
    p.notable_work = row.notable_work;

// --- Import Award nodes ---
LOAD CSV WITH HEADERS FROM 'file:///Award.csv' AS row
MERGE (a:Award {name: row.name})
SET a.id = row.id;

// --- Import AwardStatement nodes ---
LOAD CSV WITH HEADERS FROM 'file:///AwardStatement.csv' AS row
MERGE (as:AwardStatement {name: row.name})
SET as.year = row.year,
    as.motivation = row.motivation;

// --- Import other entity nodes ---
LOAD CSV WITH HEADERS FROM 'file:///Country.csv' AS row
MERGE (:Country {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///Occupation.csv' AS row
MERGE (:Occupation {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///Field.csv' AS row
MERGE (:Field {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///Organization.csv' AS row
MERGE (:Organization {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///Position.csv' AS row
MERGE (:Position {name: row.name});


// --- Import relationships ---
LOAD CSV WITH HEADERS FROM 'file:///Relations.csv' AS row
CALL {
  WITH row
  // Person -[RECEIVED]-> AwardStatement
  WHERE row.relation = 'RECEIVED'
  MATCH (p:Person {name: row.start_id})
  MATCH (a:AwardStatement {name: row.end_name})
  MERGE (p)-[:RECEIVED]->(a)
  RETURN 0 AS dummy

  UNION

  // AwardStatement -[IS_INSTANCE_OF]-> Award
  WITH row
  WHERE row.relation = 'IS_INSTANCE_OF'
  MATCH (as:AwardStatement {name: row.start_id})
  MATCH (a:Award {name: row.end_name})
  MERGE (as)-[:IS_INSTANCE_OF]->(a)
  RETURN 0 AS dummy

  UNION

  // Person -[IS_CITIZEN_OF]-> Country
  WITH row
  WHERE row.relation = 'IS_CITIZEN_OF'
  MATCH (p:Person {name: row.start_id})
  MATCH (c:Country {name: row.end_name})
  MERGE (p)-[:IS_CITIZEN_OF]->(c)
  RETURN 0 AS dummy

  UNION

  // Person -[WORKS_AS]-> Occupation
  WITH row
  WHERE row.relation = 'WORKS_AS'
  MATCH (p:Person {name: row.start_id})
  MATCH (o:Occupation {name: row.end_name})
  MERGE (p)-[:WORKS_AS]->(o)
  RETURN 0 AS dummy

  UNION

  // Person -[WORKS_IN_FIELD]-> Field
  WITH row
  WHERE row.relation = 'WORKS_IN_FIELD'
  MATCH (p:Person {name: row.start_id})
  MATCH (f:Field {name: row.end_name})
  MERGE (p)-[:WORKS_IN_FIELD]->(f)
  RETURN 0 AS dummy

  UNION

  // Person -[EDUCATED_AT]-> Organization
  WITH row
  WHERE row.relation = 'EDUCATED_AT'
  MATCH (p:Person {name: row.start_id})
  MATCH (org:Organization {name: row.end_name})
  MERGE (p)-[:EDUCATED_AT]->(org)
  RETURN 0 AS dummy

  UNION

  // Person -[EMPLOYED_BY]-> Organization
  WITH row
  WHERE row.relation = 'EMPLOYED_BY'
  MATCH (p:Person {name: row.start_id})
  MATCH (org:Organization {name: row.end_name})
  MERGE (p)-[:EMPLOYED_BY]->(org)
  RETURN 0 AS dummy

  UNION

  // Person -[IS_MEMBER_OF]-> Organization
  WITH row
  WHERE row.relation = 'IS_MEMBER_OF'
  MATCH (p:Person {name: row.start_id})
  MATCH (m:Organization {name: row.end_name})
  MERGE (p)-[:IS_MEMBER_OF]->(m)
  RETURN 0 AS dummy

  UNION

  // Person -[HOLDS_POSITION]-> Position
  WITH row
  WHERE row.relation = 'HOLDS_POSITION'
  MATCH (p:Person {name: row.start_id})
  MATCH (pos:Position {name: row.end_name})
  MERGE (p)-[:HOLDS_POSITION]->(pos)
  RETURN 0 AS dummy
}
RETURN count(*) AS imported;
