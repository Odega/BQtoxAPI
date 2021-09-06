# An example script showing the functionality of the TinCanPython Library

import uuid
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import datetime
import os
#Endre dette når du får fra Marcin!!! 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="../../instance/mcourser-europe-7.json"

#from test.resources import lrs_properties
from resources import lrs_properties
from tincan import (
    RemoteLRS,
    Statement,
    Agent,
    Verb,
    Activity,
    Context,
    Result,
    LanguageMap,
    ActivityDefinition,
    ActivityList,
    ContextActivities,
    Extensions,
    StateDocument,
)
# construct an LRS
print("constructing the LRS...")
lrs = RemoteLRS(
    version=lrs_properties.version,
    endpoint=lrs_properties.endpoint,
    username=lrs_properties.username,
    password=lrs_properties.password,
)
################################

def createStatement(username, email, schoolName, modifiedDate, LmKapSide, lm, kap, side, scaled, raw, max, checks, errors, mistakes, time, skoleId, skoleEier, skoleEierId, fagkartKap, fagkartSide):

    # construct the actor of the statement
    actor = Agent(
        name=username,
        mbox=email,
    )

    # construct the verb of the statement
    verb = Verb(
        id='http://adlnet.gov/expapi/verbs/answered',
        display=LanguageMap({'en-US': 'answered','no-NB':'besvarte'}),
    )

    # construct the object of the statement
    object = Activity(
        id='https://min.kunnskap.no/{}'.format(LmKapSide),
        definition=ActivityDefinition(
            name=LanguageMap({'en-US': LmKapSide}),
            description=LanguageMap({'en-US': 'Course, Lesson, Page'}),
            type='https://w3id.org/xapi/acrossx/activities/page'
        ),
    )

    ###print("constructing the Result...")
    result = Result(
        duration=time,
        response="User Input",
        success=True,
        score = {
            "min": 0.0,
            "max": max,
            "raw": raw,
            "scaled": scaled
        },
        extensions = {
            "https://w3id.org/xapi/avt/result-extensions/mistakes": mistakes,
            "https://w3id.org/xapi/avt/result-extensions/errors" : errors,
            "https://w3id.org/xapi/avt/result-extensions/checks": checks
        }
    )
    # construct a context for the statement
    context = Context(
        registration=uuid.uuid4(),
        platform= "CyberBook AS", # ELLER "min.kunnskap.no - Portal"
        language="nb-NO",
        instructor=Agent(
            name="CyberBook AS",
            mbox='mailto:kunnskap@kunnskap.no',
        ),
        contextActivities = ContextActivities(
            grouping = ActivityList(
                 # PROVIDER
                 [Activity(
                    id = "https://clientadmin.dataporten-api.no/clients/50f08e1f-3481-428f-b44f-44d2f3bfeba0",
                    definition = ActivityDefinition(
                        name=LanguageMap({'nb-NO': "CyberBook AS"}),
                        description=LanguageMap({'nb-NO': "CyberBook leverer gjennom min.kunnskap.no et interaktivt læringsmiljø for elever og lærere. CyberBooks nettbaserte læremidler på Kunnskap.no er fleksible og anvendelige. De kartlegger, differensierer  og retter. Som lærer får du automatisk tilgang til  detaljerte elevrapporter som viser deg hvor lenge  eleven har jobbet, hva eleven har jobbet med og når  og hvordan oppgavene er løst. Her finner du læremidler grunnskole, videregående og voksenopplæring i basisfagene og norsk for"}),
                        type =  "https://w3id.org/xapi/avt/activity-types/feide-clientinfo",
                    )
                ),
                # KOMMUNE (SKOLEEIER)
                Activity(
                    id=skoleEierId,
                    definition = ActivityDefinition(
                        name=LanguageMap({'nb-NO': skoleEier}),
                        type="https://w3id.org/xapi/avt/activity-types/school-owner",
                    )
                ),
                # SKOLE
                Activity(
                    id=skoleId,
                    definition = ActivityDefinition(
                        name=LanguageMap({'nb-NO': schoolName}),
                        type="https://w3id.org/xapi/avt/activity-types/school",
                    )
                ),
                 # LÆREMIDDEL
                Activity(
                    id='https://min.kunnskap.no/{}'.format(lm),
                    definition=ActivityDefinition(
                        name=LanguageMap({'nb-NO': lm}),
                        description=LanguageMap({'en-US': 'Course','nb-NO':'Læremiddel'}),
                        type='http://adlnet.gov/expapi/activities/course'
                    ),
                ),
                # FAGKARTKAP 
                Activity(
                    id=fagkartSide,
                    definition = ActivityDefinition(
                        name=LanguageMap({
                            'nb': "Et sett med parametere som knytter en oppgave/item til en eller flere referansemodeller som er merket ved bruk av Fagkartkoderverktøyet (se fagkart.no)",
                            "en":"A set of parameters that link an exercise/item to one or more reference models tagged using the fagkartkoder tool (see fagkart.no)"
                            }),
                        type="https://w3id.org/xapi/avt/activity-types/fagkart_tag",
                    )
                )]
            )  
        )
    )


    # construct the actual statement
    statement = Statement(
        actor=actor,
        verb=verb,
        object=object,
        result=result,
        context=context,
    )
    return statement
#################################

# Send statements to LRS
def sendStatements(arr):
    response = lrs.save_statements(arr)
    if not response:
        raise ValueError("statements failed to save")
    else:
        print(response)
    print("...done - statements sendt til LRS")

def secToPTMS(s):
    seconds = int(s)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    ret = "PT{}H{}M{}S".format(h,m,s)
    return ret

def combineObject(l,k,s):
    return l+"/"+k+"/"+s

def getUserName(uname):
    x = uname.split(":")
    y = x[1].split("@")
    un = y[0] 
    em = x[1]
    return un, em

# Gets todays table from BigQuery, if it exsists. Else it raises a NotFound error
def getTableFromBQ():
    datoTid = datetime.datetime.today().strftime('%Y%m%d')
    print("DatoTid:" + datoTid)
    client = bigquery.Client()
    try:
        query_job = client.query("""
           #STANDARDSQL
            SELECT 
            username, modifiedDate, lmNavn, kapNavn, sideNavn, scaledScore, rawScore, rawScore, maxScore, checks, errors, mistakes, timeSec, skoleNavn, Skole_id, Skoleeier, Skoleeier_id, tbl2.string_field_2 AS fagkartKap, tbl2.string_field_3 AS fagkartSide
            FROM (
            SELECT
            username as username,
            modified_date as modifiedDate,
            course_title as lmNavn,
            lesson_title as kapNavn,
            side.page_name as sideNavn,
            side.score as scaledScore,
            side.absolute_score as rawScore,
            side.max_score as maxScore,
            side.checks_count as checks,
            side.errors_count as errors,
            side.mistake_count as mistakes,
            side.total_time/1000 as timeSec,
            tbl3.string_field_1 AS skoleNavn, 
            tbl3.string_field_2 AS Skole_id,
            tbl3.string_field_3 AS Skoleeier,
            tbl3.string_field_4 AS Skoleeier_id
            FROM(
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY score_id ORDER BY modified_date DESC) AS row_number
            FROM `mcourser-europe-7.scores.scores_2021*` AS tbl1
            WHERE
            school_name IN ('Oslo kommune BAK', 'Oslo kommune BRG', 'Oslo kommune BOT', 'Oslo kommune BRA', 'Oslo kommune BOL', 'Oslo kommune FEN', 'Oslo kommune GOD', 'Oslo kommune HLM', 'Oslo kommune KAS', 'Oslo kommune KRI', 'Oslo kommune LOF', 'Oslo kommune LRN', 'Oslo kommune MID', 'Oslo kommune NOS', 'Oslo kommune LOR', 'Oslo kommune RUS', 'Oslo kommune RUT', 'Oslo kommune ROD', 'Oslo kommune SKA', 'Oslo kommune SOB', 'Oslo kommune TOK', 'Oslo kommune ULL', 'Oslo kommune URA', 'Oslo kommune VEI', 'Oslo kommune VOT', 'Oslo kommune VOL', 'Oslo kommune ROS', 'Oslo kommune UVG', 'Oslo kommune VHV', 'Oslo kommune SME', 'Oslo kommune SKD', 'Oslo kommune KOV', 'Oslo kommune ETT', 'Oslo kommune NYD')
            AND entry_type LIKE 'lesson'
            AND score >= 0 AND total_time > 0) as lasttbl
            INNER JOIN `mcourser-europe-7.scores.skoleeiere` AS tbl3
            ON tbl3.string_field_0 = lasttbl.school_name
            LEFT JOIN 
            UNNEST(page_score) AS side
            WHERE
            (row_number = 1)
            ) AS tbl4
            INNER JOIN `mcourser-europe-7.scores.fagkart5` AS tbl2
            ON tbl2.string_field_0 = tbl4.kapNavn AND tbl2.string_field_1 = tbl4.sideNavn
            LIMIT 2;
        """)
        results = query_job.result()  # Waits for job to complete.

        print("Hentet fra BQ...")
        return(results)
    except NotFound as err:
        print("Fant ikke Tabell i BigQuery")
        print(err)
        return(None)
#####################################################################################################
def initiatexAPI(BQobj):
    print("Initate BQ til xAPI array...")
    xAPIArr = []
    if(BQobj.total_rows == 0):
        print("INGEN RADER I BQ.. Avslutter.. ")
    else:
        ### createStatement(username, schoolName, modifiedDate, LmKapSide, scaled, raw, max, checks, errors, mistakes, time):
        for index, row in enumerate(BQobj):
            print("Kjører: {}".format(index+1))
            time = secToPTMS(row.timeSec)
            objName = combineObject(row.lmNavn, row.kapNavn, row.sideNavn)
            uname, email = getUserName(row.username)
            stmt = createStatement(uname, email, row.skoleNavn, row.modifiedDate, objName, row.lmNavn, row.kapNavn, row.sideNavn, row.scaledScore, row.rawScore, row.maxScore, row.checks, row.errors, row.mistakes, time, row.Skole_id, row.Skoleeier, row.Skoleeier_id, row.fagkartKap, row.fagkartSide)
            xAPIArr.append(stmt)
        else:
            sendStatements(xAPIArr)

if __name__ == '__main__':
    res = getTableFromBQ()
    initiatexAPI(res)
