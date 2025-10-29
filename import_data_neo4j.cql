MATCH (n) DETACH DELETE n;

// --- Import Person nodes ---
LOAD CSV WITH HEADERS FROM 'file:///Person.csv' AS row
MERGE (p:Person {name: row.name})
SET p.id = row.id;

// --- Import Award nodes ---
LOAD CSV WITH HEADERS FROM 'file:///Award.csv' AS row
MERGE (a:Award {name: row.name})
SET a.id = row.id;

// --- Import AwardStatement nodes ---
LOAD CSV WITH HEADERS FROM 'file:///AwardStatement.csv' AS row
MERGE (as:AwardStatement {name: row.name})
SET as.year = row.year,
    as.motivation = row.motivation;

// --- Import Country, Occupation, Field, Organization, MemberOrg ---
LOAD CSV WITH HEADERS FROM 'file:///Country.csv' AS row
MERGE (:Country {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///Occupation.csv' AS row
MERGE (:Occupation {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///Field.csv' AS row
MERGE (:Field {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///Organization.csv' AS row
MERGE (:Organization {name: row.name});

LOAD CSV WITH HEADERS FROM 'file:///MemberOrg.csv' AS row
MERGE (:MemberOrg {name: row.name});


// --- Import relationships ---
LOAD CSV WITH HEADERS FROM 'file:///Relations.csv' AS row
CALL (row) {
  // Person -[RECEIVED]-> AwardStatement
  WITH row
  WHERE row.relation = 'RECEIVED'
  MATCH (p:Person {name: row.start_id})
  MATCH (a:AwardStatement {name: row.end_name})
  MERGE (p)-[:RECEIVED]->(a)
  RETURN 0 AS dummy
  UNION

  // AwardStatement -[INSTANCE_OF]-> Award
  WITH row
  WHERE row.relation = 'INSTANCE_OF'
  MATCH (as:AwardStatement {name: row.start_id})
  MATCH (a:Award {name: row.end_name})
  MERGE (as)-[:INSTANCE_OF]->(a)
  RETURN 0 AS dummy
  UNION

  // Person -[CITIZEN_OF]-> Country
  WITH row
  WHERE row.relation = 'CITIZEN_OF'
  MATCH (p:Person {name: row.start_id})
  MATCH (c:Country {name: row.end_name})
  MERGE (p)-[:CITIZEN_OF]->(c)
  RETURN 0 AS dummy
  UNION

  // Person -[OCCUPATION]-> Occupation
  WITH row
  WHERE row.relation = 'OCCUPATION'
  MATCH (p:Person {name: row.start_id})
  MATCH (o:Occupation {name: row.end_name})
  MERGE (p)-[:OCCUPATION]->(o)
  RETURN 0 AS dummy
  UNION

  // Person -[FIELD_OF_WORK]-> Field
  WITH row
  WHERE row.relation = 'FIELD_OF_WORK'
  MATCH (p:Person {name: row.start_id})
  MATCH (f:Field {name: row.end_name})
  MERGE (p)-[:FIELD_OF_WORK]->(f)
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

  // Person -[EMPLOYED_AT]-> Organization
  WITH row
  WHERE row.relation = 'EMPLOYED_AT'
  MATCH (p:Person {name: row.start_id})
  MATCH (org:Organization {name: row.end_name})
  MERGE (p)-[:EMPLOYED_AT]->(org)
  RETURN 0 AS dummy
  UNION

  // Person -[MEMBER_OF]-> MemberOrg
  WITH row
  WHERE row.relation = 'MEMBER_OF'
  MATCH (p:Person {name: row.start_id})
  MATCH (m:MemberOrg {name: row.end_name})
  MERGE (p)-[:MEMBER_OF]->(m)
  RETURN 0 AS dummy
}
RETURN count(*) AS imported;