CREATE OR REPLACE PROCEDURE             "UM_F_EXT_TESTSCORE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_EXT_TESTSCORE
--V01 12/11/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_EXT_TESTSCORE';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_EXT_TESTSCORE');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_EXT_TESTSCORE disable constraint PK_UM_F_EXT_TESTSCORE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_EXT_TESTSCORE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_EXT_TESTSCORE';				
insert /*+ append */ into UM_F_EXT_TESTSCORE 
with K as 
(SELECT /*+ INLINE PARALLEL(8) */ DISTINCT APPLCNT_SID PERSON_SID, SRC_SYS_ID
   FROM PS_F_ADM_APPL_STAT
  UNION
 SELECT /*+ INLINE PARALLEL(8) */ DISTINCT PERSON_SID, SRC_SYS_ID 
   FROM UM_F_ACAD_PROG
  WHERE PERSON_SID <> 2147483646
  UNION
 SELECT /*+ INLINE PARALLEL(8) */ DISTINCT PERSON_SID, SRC_SYS_ID 
   FROM UM_D_PRSPCT_CAR
  where DATA_ORIGIN <> 'D'),        -- Aug 2019 
T1 as
(select /*+ INLINE PARALLEL(8) */
 K.PERSON_SID,
 nvl(EXT_TST_CMPNT_SID,2147483646) EXT_TST_CMPNT_SID,
 nvl(EXT_TST_DT,to_date('01-JAN-1900')) EXT_TST_DT,
 nvl(TST_DATA_SRC_SID,2147483646) TST_DATA_SRC_SID,
 K.SRC_SYS_ID,
 nvl(EMPLID,'-') PERSON_ID,  
 nvl(EXT_TST_ID,'-') TEST_ID,  
 nvl(EXT_TST_CMPNT_ID,'-') TEST_CMPNT_ID, 
 nvl(TST_DATA_SRC_ID,'-') TEST_DATA_SRC_ID,
 nvl(EXT_ACAD_LVL_SID,2147483646) EXT_ACAD_LVL_SID,
 NUMERIC_SCORE,
 (case when NUMERIC_SCORE between MIN_SCORE and MAX_SCORE then NUMERIC_SCORE else 0 end) VALID_TEST_SCORE,
 LETTER_SCORE,
 SCORE_PERCENTILE,
 LOAD_DT,
 TEST_ADMIN,
 TEST_INDEX,
 MAX_SCORE,
 MIN_SCORE,
 nvl(CONV_FLG,'N') CONV_FLG
  from K
  left outer join PS_F_EXT_TESTSCORE F
    on K.PERSON_SID = F.PERSON_SID
   and K.SRC_SYS_ID = F.SRC_SYS_ID
   and nvl(F.DATA_ORIGIN,'-') <> 'D'    -- Aug 2019 
),
T2 as
(select /*+ INLINE PARALLEL(8) */
 PERSON_SID,
 EXT_TST_CMPNT_SID,
 EXT_TST_DT,
 TST_DATA_SRC_SID,
 SRC_SYS_ID,
 PERSON_ID,  
 TEST_ID,  
 TEST_CMPNT_ID, 
 TEST_DATA_SRC_ID,
 EXT_ACAD_LVL_SID,
 NUMERIC_SCORE,
 VALID_TEST_SCORE,
 LETTER_SCORE,
 SCORE_PERCENTILE,
 LOAD_DT,
 TEST_ADMIN,
 TEST_INDEX,
 MAX_SCORE,
 MIN_SCORE,
 CONV_FLG,
 ROW_NUMBER() OVER (PARTITION BY PERSON_SID, TEST_ID, SRC_SYS_ID
                        ORDER BY EXT_TST_DT DESC, EXT_TST_CMPNT_SID DESC, TST_DATA_SRC_SID DESC) TEST_ID_DT_ORDER,
 SUM(case when TEST_ID = 'GRE' and TEST_CMPNT_ID = 'ANLY' 
           and ((VALID_TEST_SCORE > 6 and EXT_TST_DT >= '01-AUG-2011')
            or  (VALID_TEST_SCORE <= 6 and EXT_TST_DT < '01-AUG-2011')) 
          then NULL
     else VALID_TEST_SCORE end) OVER (PARTITION BY PERSON_SID, TEST_ID, EXT_TST_DT, SRC_SYS_ID) TEST_SUM, 
 CASE WHEN (TEST_DATA_SRC_ID = 'ACT')
        OR (TEST_DATA_SRC_ID = 'TSC' AND TEST_ID = 'IELTS')
        OR (TEST_DATA_SRC_ID = 'ETS' AND TEST_ID <> 'IELTS') THEN 1
      WHEN (TEST_DATA_SRC_ID = 'TSC' AND TEST_ID <> 'IELTS' AND TEST_ID <> 'MTEL')
        OR (TEST_DATA_SRC_ID = 'ETS' AND TEST_ID = 'IELTS')
        OR (TEST_DATA_SRC_ID = 'APP' AND TEST_ID = 'MTEL') THEN 2
      WHEN (TEST_DATA_SRC_ID = 'APP' AND TEST_ID <> 'MTEL')
        OR (TEST_DATA_SRC_ID = 'TSC' AND TEST_ID = 'MTEL') THEN 3
      WHEN (TEST_DATA_SRC_ID = 'INT') THEN 4
      WHEN (TEST_DATA_SRC_ID = 'CNV') THEN 5
      WHEN (TEST_DATA_SRC_ID = 'ADF') THEN 6
      WHEN (TEST_DATA_SRC_ID = 'WAP') THEN 7
      WHEN (TEST_DATA_SRC_ID = 'LSC') THEN 8
      ELSE 9
  END TEST_SOURCE_ORDER
  from T1
),
T3 as
(select /*+ INLINE PARALLEL(8) */
 PERSON_SID,
 EXT_TST_CMPNT_SID,
 EXT_TST_DT,
 TST_DATA_SRC_SID,
 SRC_SYS_ID,
 PERSON_ID, 
 TEST_ID, 
 TEST_CMPNT_ID, 
 TEST_DATA_SRC_ID,
 EXT_ACAD_LVL_SID,
 NUMERIC_SCORE,
 decode(LETTER_SCORE,'-',NULL,LETTER_SCORE) LETTER_SCORE,
 decode(SCORE_PERCENTILE,0,NULL,SCORE_PERCENTILE) SCORE_PERCENTILE,
 LOAD_DT,
 decode(TEST_ADMIN,'-',NULL,TEST_ADMIN) TEST_ADMIN,
 decode(TEST_INDEX,0,NULL,TEST_INDEX) TEST_INDEX,
 MAX_SCORE,
 MIN_SCORE,
 CONV_FLG,
 ROW_NUMBER() OVER (PARTITION BY PERSON_SID, EXT_TST_CMPNT_SID, SRC_SYS_ID 
                        ORDER BY VALID_TEST_SCORE DESC NULLS LAST, EXT_TST_DT DESC, TEST_SOURCE_ORDER, TST_DATA_SRC_SID) TEST_CMPNT_ORDER,
 ROW_NUMBER () OVER (PARTITION BY PERSON_SID, EXT_TST_CMPNT_SID, SRC_SYS_ID
                         ORDER BY TEST_ID_DT_ORDER) TEST_DT_ORDER, 
 ROW_NUMBER () OVER (PARTITION BY PERSON_SID, EXT_TST_CMPNT_SID, SRC_SYS_ID
                         ORDER BY TEST_SUM DESC, TEST_ID_DT_ORDER) TEST_SUM_ORDER,
 TEST_SOURCE_ORDER 
 from T2
)
 select /*+ PARALLEL(8) */
 PERSON_SID,
 EXT_TST_CMPNT_SID,
 EXT_TST_DT,
 TST_DATA_SRC_SID,
 SRC_SYS_ID,
 PERSON_ID, 
 TEST_ID, 
 TEST_CMPNT_ID, 
 TEST_DATA_SRC_ID,
 EXT_ACAD_LVL_SID,
 NUMERIC_SCORE,
 LETTER_SCORE,
 SCORE_PERCENTILE,
 LOAD_DT,
 TEST_ADMIN,
 TEST_INDEX,
 MAX_SCORE,
 MIN_SCORE,
 TEST_CMPNT_ORDER,
 TEST_DT_ORDER, 
 TEST_SUM_ORDER,
 TEST_SOURCE_ORDER,
          (CASE
              WHEN TEST_CMPNT_ORDER = 1
               AND EXT_TST_DT > '01-JAN-1900' 
               AND (NUMERIC_SCORE between MIN_SCORE and MAX_SCORE
                    or MIN_SCORE - MAX_SCORE = 0) THEN 'Y'
              ELSE 'N'
           END) BEST_SCORE_FLG,
          (CASE
              WHEN TEST_CMPNT_ORDER = 1
               AND EXT_TST_DT > '01-JAN-1900' 
               AND (NUMERIC_SCORE between MIN_SCORE and MAX_SCORE
                    or MIN_SCORE - MAX_SCORE = 0) THEN 'Y'
              ELSE 'N'
           END) HIGHEST_SCORE_FLG,
          CASE
             WHEN     (DENSE_RANK ()
                       OVER (
                          PARTITION BY PERSON_SID,
                                       EXT_TST_CMPNT_SID,
                                       SRC_SYS_ID
                          ORDER BY
                             PERSON_SID,
                             EXT_TST_CMPNT_SID,
                             SRC_SYS_ID,
                             EXT_TST_DT DESC)) = 1
                  AND EXT_TST_DT > '01-JAN-1900'
             THEN
                'Y'
             ELSE
                'N'
          END
             LATEST_TEST_DT_FLG,
 CONV_FLG
 from T3
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_EXT_TESTSCORE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_EXT_TESTSCORE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_EXT_TESTSCORE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_EXT_TESTSCORE enable constraint PK_UM_F_EXT_TESTSCORE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_EXT_TESTSCORE');

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

END UM_F_EXT_TESTSCORE_P;
/
