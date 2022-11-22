DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_CHK_P
/

--
-- UM_F_ADM_APPL_CHK_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ADM_APPL_CHK_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_CHK
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_CHK';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_CHK';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_CHK');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_CHK disable constraint PK_UM_F_ADM_APPL_CHK';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_CHK';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_F_ADM_APPL_CHK
   WITH ADM
        AS (SELECT /*+ parallel(8) inline */ DISTINCT
                   APPLCNT_SID, ADM_APPL_NBR, SRC_SYS_ID
              FROM UM_F_ADM_APPL_STAT),
        VAR
        AS (SELECT /*+ parallel(8) inline */ DISTINCT
                   COMMON_ID, ADM_APPL_NBR, VAR_DATA_SID
              FROM PS_D_VAR_DATA
             WHERE ADMIN_FUNCTION = 'ADMP' AND DATA_ORIGIN <> 'D'),
        CHK
        AS (SELECT /*+ parallel(8) inline */
                   F.PERSON_SID,
                   V.ADM_APPL_NBR,
                   F.COMMON_ID,
                   F.SEQ_3C,
                   R.CHECKLIST_SEQ,
                   F.SRC_SYS_ID,
--                   F.CHKLIST_DT_SID,
                   F.CHKLIST_DT,
                   F.CHKLIST_TM,
                   F.ADMIN_FUNC_SID,
                   F.INSTITUTION_SID,
                   F.CHKLIST_CD_SID,
                   F.CHKLIST_STAT_SID,
--                   F.STATUS_DT_SID,
                   F.STATUS_DT,
--                   F.DUE_DT_SID,
                   F.DUE_DT,
                   F.RESPONSIBLE_SID,
                   F.VAR_DATA_SID,
                   F.TRACKING_SEQ,
                   F.DUE_AMT,
                   R.ITEM_CD_SID,
                   R.ITEM_STATUS,
                   R.ITEM_STATUS_SD,
                   R.ITEM_STATUS_LD,
                   R.STATUS_DT ITEM_STATUS_DT,
                   R.STATUS_CHANGE_ID ITEM_STATUS_CHANGE_ID,
                   R.DUE_DT ITEM_DUE_DT,
                   R.DUE_AMT ITEM_DUE_AMT,
                   R.RESPONSIBLE_ID ITEM_RESPONSIBLE_ID,
                   R.ASSOC_ID,
                   R.NAME,
                   R.COMM_KEY,
                   DENSE_RANK () OVER (PARTITION BY F.PERSON_SID, V.ADM_APPL_NBR, F.SRC_SYS_ID
                                           ORDER BY F.SEQ_3C DESC) CHKLIST_ORDER
              FROM PS_F_CHKLST_PERSON F
              JOIN PS_R_CHKLST_ITEM R
                ON F.COMMON_ID = R.COMMON_ID
               AND F.SEQ_3C = R.SEQ_3C
               AND F.SRC_SYS_ID = R.SRC_SYS_ID
               --AND F.ADMIN_FUNC_AREA IN ('ADM', 'EVNT') NJ- I do not believe this is required
               AND F.DATA_ORIGIN <> 'D'
               AND R.DATA_ORIGIN <> 'D'
              JOIN VAR V
                ON F.VAR_DATA_SID = V.VAR_DATA_SID)
   SELECT ADM.APPLCNT_SID,
          ADM.ADM_APPL_NBR,
          nvl(CHK.SEQ_3C,0) SEQ_3C,
          nvl(CHK.CHECKLIST_SEQ,0) CHECKLIST_SEQ,
          ADM.SRC_SYS_ID,
          nvl(CHK.COMMON_ID,'-') COMMON_ID,
          NVL (CHK.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
          NVL (CHK.ADMIN_FUNC_SID, 2147483646) ADMIN_FUNC_SID,
          NVL (CHK.CHKLIST_CD_SID, 2147483646) CHKLIST_CD_SID,
          NVL (CHK.CHKLIST_STAT_SID, 2147483646) CHKLIST_STAT_SID,
          NVL (CHK.ITEM_CD_SID, 2147483646) ITEM_CD_SID,
          NVL (CHK.RESPONSIBLE_SID, 2147483646) RESPONSIBLE_SID,
          NVL (CHK.VAR_DATA_SID, 2147483646) VAR_DATA_SID,
--          TO_DATE (CHK.CHKLIST_DT_SID, 'YYYYMMDD') CHKLIST_DT,
          CHK.CHKLIST_DT,
          CHK.CHKLIST_TM,
          CHK.DUE_AMT,
--          TO_DATE (CHK.DUE_DT_SID, 'YYYYMMDD') DUE_DT,
          CHK.DUE_DT,
          CHK.ITEM_DUE_DT,
          CHK.DUE_AMT ITEM_DUE_AMT,
          CHK.ITEM_RESPONSIBLE_ID,
          CHK.ITEM_STATUS,
          CHK.ITEM_STATUS_SD,
          CHK.ITEM_STATUS_LD,
          CHK.ITEM_STATUS_DT,
          CHK.ITEM_STATUS_CHANGE_ID,
--          TO_DATE (CHK.STATUS_DT_SID, 'YYYYMMDD') STATUS_DT,
          CHK.STATUS_DT,
          CHK.TRACKING_SEQ,
          CHK.ASSOC_ID,
          CHK.NAME,
          CHK.COMM_KEY,
          CHK.CHKLIST_ORDER
     FROM ADM
     LEFT OUTER JOIN CHK
       ON ADM.APPLCNT_SID = CHK.PERSON_SID
      AND ADM.SRC_SYS_ID = CHK.SRC_SYS_ID
      AND ADM.ADM_APPL_NBR = CHK.ADM_APPL_NBR
    where ADM.APPLCNT_SID <> 2147483646
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_CHK rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_CHK',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_CHK';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_CHK enable constraint PK_UM_F_ADM_APPL_CHK';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_CHK');

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

END UM_F_ADM_APPL_CHK_P;
/
