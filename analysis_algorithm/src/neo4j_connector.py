from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")


def get_driver():
    """Return a neo4j driver using credentials from .env."""
    if not NEO4J_URI or not NEO4J_USER or not NEO4J_PASSWORD:
        raise RuntimeError("NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD must be set in .env")
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def fetch_person_relations(driver):
    """
    Fetch Person nodes and their related entities used to create a projected
    person-person graph. Returns a dict with:
      { 'persons': {pid: name, ...},
        'relations': { relation_type: { entity_name: set([pid,...]) } }
      }
    Relation groups collected: organization (EDUCATED_AT, EMPLOYED_BY, IS_MEMBER_OF),
    field (WORKS_IN_FIELD), country (IS_CITIZEN_OF), awardstatement (co-recipients).
    """
    persons = {}
    relations = {
        'organization': {},
        'field': {},
        'country': {},
        'awardstatement': {}
    }

    with driver.session() as session:
        # Persons
        res = session.run("MATCH (p:Person) RETURN p.id AS id, p.name AS name")
        for r in res:
            pid = r.get('id') or r.get('name')
            persons[pid] = r.get('name')

        # Organizations: EDUCATED_AT, EMPLOYED_BY, IS_MEMBER_OF
        q_org = """
        MATCH (p:Person)-[r]->(o:Organization)
        WHERE type(r) IN ['EDUCATED_AT','EMPLOYED_BY','IS_MEMBER_OF']
        RETURN p.id AS pid, o.name AS ename
        """
        res = session.run(q_org)
        for r in res:
            pid = r.get('pid') or r.get('ename')
            en = r.get('ename')
            if not pid or not en:
                continue
            relations['organization'].setdefault(en, set()).add(pid)

        # Fields
        q_field = "MATCH (p:Person)-[:WORKS_IN_FIELD]->(f:Field) RETURN p.id AS pid, f.name AS fname"
        res = session.run(q_field)
        for r in res:
            pid = r.get('pid')
            en = r.get('fname')
            if not pid or not en:
                continue
            relations['field'].setdefault(en, set()).add(pid)

        # Countries
        q_country = "MATCH (p:Person)-[:IS_CITIZEN_OF]->(c:Country) RETURN p.id AS pid, c.name AS cname"
        res = session.run(q_country)
        for r in res:
            pid = r.get('pid')
            en = r.get('cname')
            if not pid or not en:
                continue
            relations['country'].setdefault(en, set()).add(pid)

        # AwardStatement co-recipients (group by award statement name+year)
        q_award = "MATCH (a:AwardStatement)-[:RECEIVED]->(p:Person) RETURN a.name AS aname, a.year AS year, p.id AS pid"
        res = session.run(q_award)
        for r in res:
            aname = r.get('aname') or ''
            year = r.get('year') or ''
            pid = r.get('pid')
            if not pid:
                continue
            key = f"{aname}_{year}"
            relations['awardstatement'].setdefault(key, set()).add(pid)

    return {'persons': persons, 'relations': relations}
