import org.neo4j.driver._
import scala.collection.JavaConverters._

object Neo4jCreateDB extends App {

    // 1. Beste rundetid i kvalifisering q3 vs i på race i dag.
    // fastest_lap_race blir null
    val bestQualifyingRoundAndRaceTime = """MATCH (d:Driver) - [re:PARTICIPATED_AT] -> (q: Qualifying {raceId: 1056})
with d, q.q3 as fastest_lap_q3
match (d:Driver) - [r: RACED_AT] -> (ra: Race {raceId: 1056})
return d.surname as Name, r.fastestLapTime as fasted_lap_race,fastest_lap_q3, ra.name as Race
order by r.fastestLapTime asc"""

    // 2. Beste rundetid i kvalifisering og race for hver fører i Monaco Grand Prix 2022.
    // fastest_lap_race blir null
    val bestRoundForEveryDriverInMonaco = """MATCH (r:Race {raceId: 1080}) <-[ra:RACED_AT]-(dr:Driver) -[re:PARTICIPATED_AT]->(q: Qualifying {raceId: 1080})
RETURN CASE
WHEN q.q2 = '\N'
    THEN q.q1
WHEN q.q3 = '\N'
   THEN q.q2
ELSE q.q3
END AS FastestLapInQ, ra.fastestLapTime as FastestLapInRace ,ra.fastestLap as FastestLapNumber, ra.rank as Rank, dr.forename as Forename, dr.surname as Surname
ORDER BY FastestLapInRace"""

    // 3. Beste rundetid i race når det regner/ikke-regner:
        // denne fungerer ikke
    val bestTimeWhenRaining = """MATCH (r:Race)<-[LAP_IN]-(l:Lap)<-[ON_LAP]-(d:Driver)
WHERE l.Rainfall = "TRUE"
RETURN r.name as Name, r.year as Year, min(l.milliseconds) as BestLapTime"""

   // 4. Denne fungerer ikke
    val bestTimeWhenNotRaining = """
MATCH (r:Race)<-[LAP_IN]-(l:Lap)<-[ON_LAP]-(d:Driver)
WHERE l.Rainfall = "FALSE"
RETURN r.name as Name, r.year as Year, min(l.milliseconds) as BestLapTime"""

    // 5. Hvilke førere har flest seire når det regner i 2022:
    val mostWinsWhenRaining = """MATCH (dr:Driver)-[ra:RACED_AT]-(r:Race {year: "2022"})-[LAP_IN]-(l:Lap)
WHERE l.Rainfall = TRUE AND ra.position = "1"
RETURN distinct r.name as RaceName, count(distinct r.name) as NumberOfWins, r.year as Year, dr.forename as Forename, dr.surname as Surname"""

    // 6. Finner beste tid for hver dekketype i alle løp:
    val bestTimeForTyreType = """MATCH (r:Race)<-[LAP_IN]-(l:Lap)<-[onLap: ON_LAP]-(d:Driver)
RETURN r.name as Name, r.year as Year, min(l.milliseconds) as BestLapTime, onLap.Compound as Compound
ORDER BY BestLapTime ASC"""

   // 7.  Teller opp hvor man pole position og seire en sjåfør har:
    val polePositionsAndVictories = """match (d:Driver) - [r: RACED_AT {position: "1"}] -> (ra: Race)
with d, count(r.position) as Wins
match (d:Driver) - [r: RACED_AT] -> (ra: Race)
where r.qualifying_position = 1
return count(r.qualifying_position) as Pole_position, Wins , d.Name as Name"""

        // Query to Neo4JDB (todo: endre til riktig localhost(ta inn som parameter, og authTokens))
    def DBQuery(script: String): Int = {  
        val driver = GraphDatabase.driver("bolt://localhost/7687", AuthTokens.basic("neo4j", "123456789"))  
        val session = driver.session   
        val result = session.run(script)  
        result.asScala.foreach(println)
        session.close()  
        driver.close()  
        result.consume().counters().nodesCreated()  
    }

    println("Query1")
    DBQuery(bestQualifyingRoundAndRaceTime)
    println("Query2")
    DBQuery(bestRoundForEveryDriverInMonaco)
    println("Query3")
    DBQuery(bestTimeWhenRaining)
    println("Query4")
    DBQuery(bestTimeWhenNotRaining)
    println("Query5")
    DBQuery(mostWinsWhenRaining)
    println("Query6")
    DBQuery(bestTimeForTyreType)
    println("Query7")
    DBQuery(polePositionsAndVictories)






}