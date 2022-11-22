DROP PROCEDURE CSMRT_OWNER.UMOL_PROD_REFRESH_P
/

--
-- UMOL_PROD_REFRESH_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UMOL_PROD_REFRESH_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
-- Loads table                -- UMOL_PROD_REFRESH
-- V01 11/05/2020             -- Jim Doucette.  Temporary refresh of UMOL Prod from UAT

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UMOL_PROD_REFRESH';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );



strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMAMH';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMAMH';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMAMH
   select *
     from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMAMH@SMTUAT
    where TIMESTAMP > (select max(TIMESTAMP) from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMAMH)
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMBOS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMBOS';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMBOS
   select *
     from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMBOS@SMTUAT
    where TIMESTAMP > (select max(TIMESTAMP) from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMBOS)
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMDAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMDAR';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMDAR
   select *
     from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMDAR@SMTUAT
    where TIMESTAMP > (select max(TIMESTAMP) from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMDAR)
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW
   select *
     from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW@SMTUAT
    where TIMESTAMP > (select max(TIMESTAMP) from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW)
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY
   select *
     from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY@SMTUAT
    where TIMESTAMP > (select max(TIMESTAMP) from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY)
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMOL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMOL';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMOL
   select *
     from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMOL@SMTUAT
    where TIMESTAMP > (select max(TIMESTAMP) from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMOL)
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMWOR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMWOR';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMWOR
   select *
     from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMWOR@SMTUAT
    where TIMESTAMP > (select max(TIMESTAMP) from CSSTG_OWNER.UMOL_ACTIVITY_ACCUMULATOR_UMWOR)
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;



 ---- ADD TRUNCATE STATEMENTS!
strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2
   select *
     from CSSTG_OWNER.UMOL_CLASSIFICATIONS_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2
   select *
     from CSSTG_OWNER.UMOL_CONFERENCE_MAIN_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_COURSE_CONTENTS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_COURSE_CONTENTS_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_COURSE_CONTENTS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_COURSE_CONTENTS_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_COURSE_CONTENTS_S2
   select *
     from CSSTG_OWNER.UMOL_COURSE_CONTENTS_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_COURSE_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_COURSE_MAIN_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_COURSE_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_COURSE_MAIN_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_COURSE_MAIN_S2
   select *
     from CSSTG_OWNER.UMOL_COURSE_MAIN_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_COURSE_TERM_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_COURSE_TERM_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_COURSE_TERM_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_COURSE_TERM_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_COURSE_TERM_S2
   select *
     from CSSTG_OWNER.UMOL_COURSE_TERM_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_FORUM_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_FORUM_MAIN_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_FORUM_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_FORUM_MAIN_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_FORUM_MAIN_S2
   select *
     from CSSTG_OWNER.UMOL_FORUM_MAIN_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_GROUPS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_GROUPS_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_GROUPS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_GROUPS_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_GROUPS_S2
   select *
     from CSSTG_OWNER.UMOL_GROUPS_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2
   select *
     from CSSTG_OWNER.UMOL_INSTITUTION_ROLES_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_TERM_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_TERM_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_TERM_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_TERM_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_TERM_S2
   select *
     from CSSTG_OWNER.UMOL_TERM_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Truncating table CSSTG_OWNER.UMOL_USERS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSSTG_OWNER.UMOL_USERS_S2';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSSTG_OWNER.UMOL_USERS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSSTG_OWNER.UMOL_USERS_S2';				
   insert /*+ append */ into CSSTG_OWNER.UMOL_USERS_S2
   select *
     from CSSTG_OWNER.UMOL_USERS_S2@SMTUAT
   ;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;



   strClientInfo   := strProcessName || ' - UMOL_ACTIVITY_ACCUMULATOR_UMAMH GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_ACTIVITY_ACCUMULATOR_UMAMH GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_ACTIVITY_ACCUMULATOR_UMAMH',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
   
   
   strClientInfo   := strProcessName || ' - UMOL_ACTIVITY_ACCUMULATOR_UMBOS GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_ACTIVITY_ACCUMULATOR_UMBOS GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_ACTIVITY_ACCUMULATOR_UMBOS',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_ACTIVITY_ACCUMULATOR_UMDAR GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_ACTIVITY_ACCUMULATOR_UMDAR GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_ACTIVITY_ACCUMULATOR_UMDAR',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strClientInfo   := strProcessName || ' - UMOL_ACTIVITY_ACCUMULATOR_UMLOW GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_ACTIVITY_ACCUMULATOR_UMLOW GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_ACTIVITY_ACCUMULATOR_UMLOW',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_ACTIVITY_ACCUMULATOR_UMLOW_DAY',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_ACTIVITY_ACCUMULATOR_UMOL GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_ACTIVITY_ACCUMULATOR_UMOL GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_ACTIVITY_ACCUMULATOR_UMOL',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_ACTIVITY_ACCUMULATOR_UMWOR GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_ACTIVITY_ACCUMULATOR_UMWOR GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_ACTIVITY_ACCUMULATOR_UMWOR',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_CLASSIFICATIONS_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_CLASSIFICATIONS_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_CLASSIFICATIONS_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

 strClientInfo   := strProcessName || ' - UMOL_CONFERENCE_MAIN_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_CONFERENCE_MAIN_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_CONFERENCE_MAIN_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_COURSE_CONTENTS_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_COURSE_CONTENTS_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_COURSE_CONTENTS_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_COURSE_MAIN_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_COURSE_MAIN_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_COURSE_MAIN_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_COURSE_TERM_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_COURSE_TERM_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_COURSE_TERM_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_FORUM_MAIN_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_FORUM_MAIN_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_FORUM_MAIN_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_GROUPS_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_GROUPS_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_GROUPS_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_INSTITUTION_ROLES_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_INSTITUTION_ROLES_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_INSTITUTION_ROLES_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_TERM_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_TERM_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_TERM_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strClientInfo   := strProcessName || ' - UMOL_USERS_S2 GATHER_TABLE_STATS';
strSqlCommand   := 'SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strClientInfo);

strSqlCommand   := 'UMOL_USERS_S2 GATHER_TABLE_STATS';
DBMS_STATS.GATHER_TABLE_STATS(
            OWNNAME                 => 'CSSTG_OWNER',
            TABNAME                 => 'UMOL_USERS_S2',
			cascade                 => TRUE,
            DEGREE                  => 16,
            estimate_percent        => DBMS_STATS.auto_sample_size,
            method_opt              => 'FOR ALL COLUMNS SIZE SKEWONLY'
                                  );

strSqlCommand   := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

---


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UMOL_PROD_REFRESH',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UMOL_PROD_REFRESH_P;
/
