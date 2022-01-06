CREATE OR REPLACE PROCEDURE             "PS_PERSON_SA_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_PERSON_SA'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_PERSON_SA', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_PERSON_SA'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_PERSON_SA from PeopleSoft table PS_PERSON_SA.
   --
   -- V01  SMT-xxxx 08/15/2017,    Jim Doucette
   --                              Converted from DataStage
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_PERSON_SA';
   intProcessSid      INTEGER;
   dtProcessStart     DATE := SYSDATE;
   strMessage01       VARCHAR2 (4000);
   strMessage02       VARCHAR2 (512);
   strMessage03       VARCHAR2 (512) := '';
   strNewLine         VARCHAR2 (2) := CHR (13) || CHR (10);
   strSqlCommand      VARCHAR2 (32767) := '';
   strSqlDynamic      VARCHAR2 (32767) := '';
   strClientInfo      VARCHAR2 (100);
   intRowCount        INTEGER;
   intTotalRowCount   INTEGER := 0;
   numSqlCode         NUMBER;
   strSqlErrm         VARCHAR2 (4000);
   intTries           INTEGER;
BEGIN
   strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
   DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT (
      i_MartId             => strMartId,
      i_ProcessName        => strProcessName,
      i_ProcessStartTime   => dtProcessStart,
      o_ProcessSid         => intProcessSid);

   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_PERSON_SA';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_PERSON_SA@SASOURCE S)
    WHERE TABLE_NAME = 'PS_PERSON_SA';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_PERSON_SA';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

strSqlCommand := 'merge into CSSTG_OWNER.PS_PERSON_SA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_PERSON_SA T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(VA_BENEFIT),'-') VA_BENEFIT, 
    nvl(trim(CAMPUS_ID),'-') CAMPUS_ID, 
    nvl(trim(DEATH_CERTIF_NBR),'-') DEATH_CERTIF_NBR, 
    nvl(trim(FERPA),'-') FERPA, 
    nvl(trim(PLACE_OF_DEATH),'-') PLACE_OF_DEATH
from SYSADM.PS_PERSON_SA@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_SA') ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.VA_BENEFIT = S.VA_BENEFIT,
    T.CAMPUS_ID = S.CAMPUS_ID,
    T.DEATH_CERTIF_NBR = S.DEATH_CERTIF_NBR,
    T.FERPA = S.FERPA,
    T.PLACE_OF_DEATH = S.PLACE_OF_DEATH,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.VA_BENEFIT <> S.VA_BENEFIT or 
    T.CAMPUS_ID <> S.CAMPUS_ID or 
    T.DEATH_CERTIF_NBR <> S.DEATH_CERTIF_NBR or 
    T.FERPA <> S.FERPA or 
    T.PLACE_OF_DEATH <> S.PLACE_OF_DEATH or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.SRC_SYS_ID, 
    T.VA_BENEFIT, 
    T.CAMPUS_ID,
    T.DEATH_CERTIF_NBR, 
    T.FERPA,
    T.PLACE_OF_DEATH, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    'CS90', 
    S.VA_BENEFIT, 
    S.CAMPUS_ID,
    S.DEATH_CERTIF_NBR, 
    S.FERPA,
    S.PLACE_OF_DEATH, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_PERSON_SA rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_PERSON_SA',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_PERSON_SA';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_PERSON_SA';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_PERSON_SA';
update CSSTG_OWNER.PS_PERSON_SA T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID
   from CSSTG_OWNER.PS_PERSON_SA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_SA') = 'Y'
  minus
 select EMPLID
   from SYSADM.PS_PERSON_SA@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_PERSON_SA') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_PERSON_SA rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_PERSON_SA',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_PERSON_SA';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

   strMessage01 := strProcessName || ' is complete.';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);
EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);
         
END PS_PERSON_SA_P;
/
