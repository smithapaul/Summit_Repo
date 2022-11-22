DROP PROCEDURE CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS_P
/

--
-- UM_F_FA_ISIR_COMMENTS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_ISIR_COMMENTS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_FA_ISIR_COMMENTS.
--
--V01   SMT-xxxx 07/25/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_ISIR_COMMENTS';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_ISIR_COMMENTS');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS disable constraint PK_UM_F_FA_ISIR_COMMENTS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_ISIR_COMMENTS
  with X as (
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       LIST1 AS (
SELECT /*+ PARALLEL(8) INLINE */
       INSTITUTION INSTITUTION_CD,
       AID_YEAR,
       EMPLID PERSON_ID,
       SRC_SYS_ID,
       MAX(TO_CHAR(EFFDT,'YYYYMMDD')||TRIM(TO_CHAR(EFFSEQ,'00000'))) EFF            -- View NK -> INSTITUTION, AID_YEAR, EMPLID, SRC_SYS_ID
  FROM CSSTG_OWNER.PS_ISIR_STUDENT
 WHERE DATA_ORIGIN <> 'D'
 GROUP BY INSTITUTION,
       AID_YEAR,
       EMPLID,
       SRC_SYS_ID),
ISIRCOMM AS (
SELECT /*+ PARALLEL(8) INLINE */ A.INSTITUTION INSTITUTION_CD, A.AID_YEAR, A.EMPLID PERSON_ID, A.COMMENT_CODE, A.SRC_SYS_ID,
       A.EFFDT, A.EFFSEQ,
       B.SFA_DB_MATCH_USE, B.SEVERITY_LVL, B.DESCRLONG
  FROM CSSTG_OWNER.PS_ISIR_COMMENTS A                  -- NK -> EMPLID, INSTITUTION, AID_YEAR, EFFDT, EFFSEQ, COMMENT_CODE, SRC_SYS_ID
  LEFT OUTER JOIN CSSTG_OWNER.PS_ISIR_COMMT_TBL B      -- NK -> AID_YEAR, COMMENT_CODE, SRC_SYS_ID
    ON A.AID_YEAR = B.AID_YEAR
   AND A.COMMENT_CODE = B.COMMENT_CODE
   AND B.DATA_ORIGIN <> 'D'
  JOIN LIST1 C
    ON A.EMPLID = C.PERSON_ID
   AND A.AID_YEAR = C.AID_YEAR
   AND A.INSTITUTION = C.INSTITUTION_CD
   AND TO_CHAR(A.EFFDT,'YYYYMMDD')||TRIM(TO_CHAR(A.EFFSEQ,'00000')) = C.EFF
 WHERE A.DATA_ORIGIN <> 'D')
SELECT /*+ PARALLEL(8) INLINE */
       I.INSTITUTION_CD,
       I.PERSON_ID,
       I.AID_YEAR,
       nvl(ISIRCOMM.COMMENT_CODE,'-') COMMENT_CODE,
       I.SRC_SYS_ID,
       ISIRCOMM.EFFDT,
       ISIRCOMM.EFFSEQ,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
       nvl(P.PERSON_SID,2147483646) PERSON_SID,
       ISIRCOMM.SFA_DB_MATCH_USE,
       ISIRCOMM.SEVERITY_LVL,
       X1.XLATLONGNAME SEVERITY_LVL_LD,
       ISIRCOMM.DESCRLONG,
       'N' LOAD_ERROR,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM,
       1234 BATCH_SID
  FROM UM_F_FA_STDNT_AID_ISIR I
  LEFT OUTER JOIN ISIRCOMM
    ON I.INSTITUTION_CD = ISIRCOMM.INSTITUTION_CD
   AND I.AID_YEAR = ISIRCOMM.AID_YEAR
   AND I.PERSON_ID = ISIRCOMM.PERSON_ID
   AND I.SRC_SYS_ID = ISIRCOMM.SRC_SYS_ID
  left outer join X X1
    on ISIRCOMM.SEVERITY_LVL = X1.FIELDVALUE
   and ISIRCOMM.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'SEVERITY_LVL'
   and X1.X_ORDER = 1
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_INSTITUTION ID
    on I.INSTITUTION_CD = ID.INSTITUTION_CD
   and I.SRC_SYS_ID = ID.SRC_SYS_ID
   and ID.DATA_ORIGIN <> 'D'
  LEFT OUTER JOIN CSMRT_OWNER.PS_D_PERSON P
    on I.PERSON_ID = P.PERSON_ID
   and I.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_ISIR_COMMENTS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_ISIR_COMMENTS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_ISIR_COMMENTS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_ISIR_COMMENTS enable constraint PK_UM_F_FA_ISIR_COMMENTS';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_ISIR_COMMENTS');

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

END UM_F_FA_ISIR_COMMENTS_P;
/
