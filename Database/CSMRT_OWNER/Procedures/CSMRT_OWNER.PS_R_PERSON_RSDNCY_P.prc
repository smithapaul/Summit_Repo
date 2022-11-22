DROP PROCEDURE CSMRT_OWNER.PS_R_PERSON_RSDNCY_P
/

--
-- PS_R_PERSON_RSDNCY_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_R_PERSON_RSDNCY_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Loads table             -- PS_R_PERSON_RSDNCY
--PS_R_PERSON_RSDNCY   -- PS_D_INSTITUTION ;PS_D_ACAD_CAR;PS_D_PERSON;PS_D_RSDNCY;PS_D_RSDNCY_EXCPT
-- V01 11/28/2018           -- srikanth ,pabbu converted to proc from sql 

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_R_PERSON_RSDNCY';
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

strMessage01    := 'Truncating table CSMRT_OWNER.PS_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_R_PERSON_RSDNCY';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.PS_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','PS_R_PERSON_RSDNCY');

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_PERSON_RSDNCY disable constraint PK_PS_R_PERSON_RSDNCY';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.PS_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_R_PERSON_RSDNCY';				
insert /*+ append parallel(8) enable_parallel_dml */ into PS_R_PERSON_RSDNCY 
  WITH APP AS (
SELECT /*+ parallel(8) inline */ EMPLID, ACAD_CAREER, INSTITUTION, EFFECTIVE_TERM, SRC_SYS_ID,
       EFFDT, APPEAL_STATUS, COMMENTS,
       ROW_NUMBER() OVER (PARTITION BY EMPLID, ACAD_CAREER, INSTITUTION, EFFECTIVE_TERM, SRC_SYS_ID
                              ORDER BY DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) APP_ORDER
  from CSSTG_OWNER.PS_RESIDENCY_APEAL A        -- NK --> EMPLID, ACAD_CAREER, INSTITUTION, EFFECTIVE_TERM, EFFDT, SRC_SYS_ID 
 WHERE A.DATA_ORIGIN <> 'D')
SELECT /*+ parallel(8) */ 
       R.INSTITUTION  as INSTITUTION_CD,
       R.ACAD_CAREER as ACAD_CAR_CD,
       R.EFFECTIVE_TERM as EFF_TERM_CD,
       R.EMPLID as PERSON_ID, 
       R.SRC_SYS_ID,
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID,
	   nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID,
	   nvl(T.TERM_SID,2147483646) EFF_TERM_SID,
	   nvl(P.PERSON_SID, 2147483646) PERSON_SID,
	   nvl(PR.RSDNCY_SID, 2147483646) RSDNCY_SID,
	   nvl(PR.RSDNCY_ID, '-') RSDNCY_ID,            -- Oct 2019 
	   nvl(PR.RSDNCY_LD, '-') RSDNCY_LD,            -- Oct 2019 
	   nvl(PR1.RSDNCY_SID, 2147483646) ADM_RSDNCY_SID,
	   nvl(PR1.RSDNCY_ID, '-') ADM_RSDNCY_ID,       -- Oct 2019 
	   nvl(PR1.RSDNCY_LD, '-') ADM_RSDNCY_LD,       -- Oct 2019 
	   nvl(PR2.RSDNCY_SID, 2147483646) FA_FED_RSDNCY_SID,
	   nvl(PR2.RSDNCY_ID, '-') FA_FED_RSDNCY_ID,    -- Oct 2019 
	   nvl(PR2.RSDNCY_LD, '-') FA_FED_RSDNCY_LD,    -- Oct 2019 
	   nvl(PR3.RSDNCY_SID, 2147483646) FA_ST_RSDNCY_SID,
	   nvl(PR3.RSDNCY_ID, '-') FA_ST_RSDNCY_ID,     -- Oct 2019 
	   nvl(PR3.RSDNCY_LD, '-') FA_ST_RSDNCY_LD,     -- Oct 2019 
	   nvl(PR4.RSDNCY_SID, 2147483646) TUITION_RSDNCY_SID,
	   nvl(PR4.RSDNCY_ID, '-') TUITION_RSDNCY_ID,   -- Oct 2019 
	   nvl(PR4.RSDNCY_LD, '-') TUITION_RSDNCY_LD,   -- Oct 2019 
	   nvl(T.TERM_SID,2147483646) RSDNCY_TERM_SID,  -- Oct 2019 
	   nvl(T.TERM_CD, '-') RSDNCY_TERM_CD,          -- Oct 2019 
	   nvl(PRE.RSDNCY_EXCPT_SID, 2147483646) ADM_EXCPT_SID,
	   nvl(PRE.RSDNCY_EXCPTN, '-') ADM_RSDNCY_EXCPTN,           -- Oct 2019 
	   nvl(PRE.RSDNCY_EXCPTN_LD, '-') ADM_RSDNCY_EXCPTN_LD,     -- Oct 2019 
	   nvl(PRE1.RSDNCY_EXCPT_SID, 2147483646) FA_FED_EXCPT_SID,
	   nvl(PRE1.RSDNCY_EXCPTN, '-') FA_RSDNCY_EXCPTN,           -- Oct 2019 
	   nvl(PRE1.RSDNCY_EXCPTN_LD, '-') FA_RSDNCY_EXCPTN_LD,     -- Oct 2019 
	   nvl(PRE2.RSDNCY_EXCPT_SID, 2147483646) FA_ST_EXCPT_SID,
	   nvl(PRE2.RSDNCY_EXCPTN, '-') FA_ST_RSDNCY_EXCPTN,        -- Oct 2019 
	   nvl(PRE2.RSDNCY_EXCPTN_LD, '-') FA_ST_RSDNCY_EXCPTN_LD,  -- Oct 2019 
	   nvl(PRE3.RSDNCY_EXCPT_SID, 2147483646) TUITION_EXCPT_SID,
	   nvl(PRE3.RSDNCY_EXCPTN, '-') TUITION_RSDNCY_EXCPTN,          -- Oct 2019 
	   nvl(PRE3.RSDNCY_EXCPTN_LD, '-') TUITION_RSDNCY_EXCPTN_LD,    -- Oct 2019 
       R.RESIDENCY_DT as RSDNCY_DT, 
       APP.EFFDT as APPEAL_EFFDT,
       NVL(APP.APPEAL_STATUS, '-') APPEAL_STATUS,
       APP.COMMENTS APP_COMMENTS,
--	   'N' LOAD_ERROR, 
       'S' DATA_ORIGIN, 
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM
--       1234 BATCH_SID
  FROM CSSTG_OWNER.PS_RESIDENCY_OFF R     -- NK --> EMPLID, ACAD_CAREER, INSTITUTION, EFFECTIVE_TERM, SRC_SYS_ID 
  LEFT OUTER JOIN APP
    ON R.EMPLID = APP.EMPLID
   AND R.ACAD_CAREER = APP.ACAD_CAREER
   AND R.INSTITUTION = APP.INSTITUTION
   AND R.EFFECTIVE_TERM = APP.EFFECTIVE_TERM
   AND R.SRC_SYS_ID = APP.SRC_SYS_ID
   AND APP.APP_ORDER = 1
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I
   on R.INSTITUTION = I.INSTITUTION_CD
   and R.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C
    on R.ACAD_CAREER = C.ACAD_CAR_CD 
   and R.INSTITUTION = C.INSTITUTION_CD	
   and R.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_TERM T	
    on R.INSTITUTION = T.INSTITUTION_CD
   and R.ACAD_CAREER = T.ACAD_CAR_CD
   and R.EFFECTIVE_TERM = T.TERM_CD
   and R.SRC_SYS_ID = T.SRC_SYS_ID 
   and T.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_PERSON P
    on R.EMPLID = P.PERSON_ID  
   and R.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_RSDNCY PR
    on R.RESIDENCY = PR.RSDNCY_ID  
   and R.SRC_SYS_ID = PR.SRC_SYS_ID
   and PR.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_RSDNCY PR1
    on R.ADMISSION_RES = PR1.RSDNCY_ID   
   and R.SRC_SYS_ID = PR1.SRC_SYS_ID
   and PR1.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_RSDNCY PR2
    on R.FIN_AID_FED_RES = PR2.RSDNCY_ID 
   and R.SRC_SYS_ID = PR2.SRC_SYS_ID
   and PR2.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_RSDNCY PR3
    on R.FIN_AID_ST_RES = PR3.RSDNCY_ID 
   and R.SRC_SYS_ID = PR3.SRC_SYS_ID
   and PR3.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_RSDNCY PR4
    on R.TUITION_RES = PR4.RSDNCY_ID 
   and R.SRC_SYS_ID = PR4.SRC_SYS_ID
   and PR4.DATA_ORIGIN <> 'D' 
  left outer join CSMRT_OWNER.PS_D_RSDNCY_EXCPT PRE
    on R.ADMISSION_EXCPT = PRE.RSDNCY_EXCPTN 
   and R.SRC_SYS_ID = PRE.SRC_SYS_ID
   and PRE.DATA_ORIGIN <> 'D'     
  left outer join CSMRT_OWNER.PS_D_RSDNCY_EXCPT PRE1
    on R.FIN_AID_FED_EXCPT = PRE1.RSDNCY_EXCPTN 
   and R.SRC_SYS_ID = PRE1.SRC_SYS_ID
   and PRE1.DATA_ORIGIN <> 'D'  
  left outer join CSMRT_OWNER.PS_D_RSDNCY_EXCPT PRE2
    on R.FIN_AID_ST_EXCPT = PRE2.RSDNCY_EXCPTN 
   and R.SRC_SYS_ID = PRE2.SRC_SYS_ID
   and PRE2.DATA_ORIGIN <> 'D'  
  left outer join CSMRT_OWNER.PS_D_RSDNCY_EXCPT PRE3
    on R.TUITION_EXCPT = PRE3.RSDNCY_EXCPTN 
   and R.SRC_SYS_ID = PRE3.SRC_SYS_ID
   and PRE3.DATA_ORIGIN <> 'D'
 WHERE R.EMPLID BETWEEN '00000000' AND '99999999'
   AND LENGTH(TRIM(R.EMPLID)) = 8
   AND R.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_R_PERSON_RSDNCY rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_PERSON_RSDNCY',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_PERSON_RSDNCY',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.PS_R_PERSON_RSDNCY';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.PS_R_PERSON_RSDNCY enable constraint PK_PS_R_PERSON_RSDNCY';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','PS_R_PERSON_RSDNCY');

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

END PS_R_PERSON_RSDNCY_P;
/
