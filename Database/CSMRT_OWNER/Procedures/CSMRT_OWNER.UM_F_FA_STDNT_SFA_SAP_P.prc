DROP PROCEDURE CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP_P
/

--
-- UM_F_FA_STDNT_SFA_SAP_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_STDNT_SFA_SAP_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_F_FA_STDNT_SFA_SAP.
--
 --V01  SMT-xxxx 07/11/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_STDNT_SFA_SAP';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_STDNT_SFA_SAP');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP disable constraint PK_UM_F_FA_STDNT_SFA_SAP';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_FA_STDNT_SFA_SAP
with STAT as (
select
INSTITUTION, ACAD_CAREER, SFA_SAP_STATUS, SRC_SYS_ID,
EFFDT, DESCRSHORT, DESCR,
row_number() over (partition by INSTITUTION, ACAD_CAREER, SFA_SAP_STATUS, SRC_SYS_ID
                       order by EFFDT desc) STAT_ORDER
from CSSTG_OWNER.PS_SFA_SAP_ST_TBL T
where DATA_ORIGIN <> 'D'
)
select /*+ parallel(8) */
S.INSTITUTION INSTITUTION_CD,
S.ACAD_CAREER ACAD_CAR_CD,
S.AID_YEAR,
S.EMPLID PERSON_ID,
--STRM,
S.PROCESS_DTTM,
S.SRC_SYS_ID,
nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID,
nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID,
nvl(E.PERSON_SID,2147483646) PERSON_SID,
nvl(G.ACAD_PROG_SID,2147483646) ACAD_PROG_SID,
nvl(P.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID,
--S.ACAD_PROG,
S.STDNT_CAR_NBR,
--S.ACAD_PLAN,
--ACAD_STANDING, --SFA_INUSE_FLAG1, --SFA_SAP_STATUS_C1, --SFA_FAIL_FLAG1, --SFA_EXCPT_USED1, --SFA_SAP_ACST_TERM, --UNT_TAKEN,
S.SFA_SAP_MAX_ATTUNT,
S.SFA_SAP_MAX_ATTFRM,
S.SFA_SAP_MAX_ATTMPT,
S.SFA_INUSE_FLAG2,
S.SFA_SAP_STATUS_C2,
S.SFA_FAIL_FLAG2,
--SFA_EXCPT_USED2, --CUM_RESIDENT_TERMS, --SFA_SAP_MAX_TRMFRM, --SFA_SAP_MAX_TERMS, --SFA_INUSE_FLAG3, --SFA_SAP_STATUS_C3, --SFA_FAIL_FLAG3, --SFA_EXCPT_USED3,
S.CUR_GPA,
--SFA_SAP_MIN_TRMFRM, --SFA_SAP_MIN_TERM, --SFA_INUSE_FLAG4, --SFA_SAP_STATUS_C4, --SFA_FAIL_FLAG4, --SFA_EXCPT_USED4,
S.CUM_GPA,
--S.SFA_SAP_MIN_CUMFRM,
S.SFA_SAP_MIN_CUM,
S.SFA_INUSE_FLAG5,
S.SFA_SAP_STATUS_C5,
S.SFA_FAIL_FLAG5,
--SFA_EXCPT_USED5, --SFA_CUR_ATT_UNITS, --SFA_CUR_ERN_UNITS, --SFA_SAP_TRM_ERNEDF, --SFA_SAP_TRM_ERNPCF, --SFA_SAP_TRM_EARNED, --SFA_SAP_TRM_EARNPC,
--SFA_INUSE_FLAG6, --SFA_SAP_STATUS_C6, --SFA_FAIL_FLAG6, --SFA_EXCPT_USED6,
S.SFA_CUM_ATT_UNITS,
S.SFA_CUM_ERN_UNITS,
--SFA_SAP_CUM_ERNEDF, --SFA_SAP_CUM_ERNPCF, --SFA_SAP_CUM_EARNED,
SFA_SAP_CUM_EARNPC,
S.SFA_INUSE_FLAG7,
S.SFA_SAP_STATUS_C7,
S.SFA_FAIL_FLAG7,
--SFA_EXCPT_USED7, --SFA_SAP_NBR_TERMS,
S.SFA_SAP_2YR_GPA,
--SFA_SAP_CUMGPA_FRM, --SFA_SAP_CUMGPA_TO, --SFA_INUSE_FLAG8, --SFA_SAP_STATUS_C8, --SFA_FAIL_FLAG8, --SFA_EXCPT_USED8, --SFA_SAP_CURERN_PCT,
SFA_SAP_CUMERN_PCT,
S.SFA_SAP_STATUS,     -- Lookup to new dim???
T1.DESCRSHORT SFA_SAP_STATUS_SD,
T1.DESCR SFA_SAP_STATUS_LD,         -- Added June 2016
S.SFA_SAP_STAT_CALC,     -- Lookup to new dim???
T2.DESCRSHORT SFA_SAP_STAT_CALC_SD,
T2.DESCR SFA_SAP_STAT_CALC_LD,      -- Added June 2016
S.SFA_UPDT_OPRID,
S.SFA_UPDT_DTTM,
S.SFA_PROCESS_OPRID,
S.SFA_SAP_PROCMSG,
trim(substr(to_char(S.SFA_SAP_COMMENTS),1,4000)) SFA_SAP_COMMENTS,
row_number() over (partition by S.INSTITUTION, S.ACAD_CAREER, S.AID_YEAR, S.EMPLID, S.SRC_SYS_ID
                       order by S.PROCESS_DTTM desc) SFA_SAP_ORDER,
'N','S',sysdate,sysdate,1234
from CSSTG_OWNER.PS_SFA_SAP_STDNT S
left outer join PS_D_INSTITUTION I
  on S.INSTITUTION = I.INSTITUTION_CD
 and S.SRC_SYS_ID = I.SRC_SYS_ID
left outer join PS_D_ACAD_CAR C
  on S.INSTITUTION = C.INSTITUTION_CD
 and S.ACAD_CAREER = C.ACAD_CAR_CD
 and S.SRC_SYS_ID = C.SRC_SYS_ID
left outer join PS_D_PERSON E
  on S.EMPLID = E.PERSON_ID
 and S.SRC_SYS_ID = E.SRC_SYS_ID
left outer join UM_D_ACAD_PROG G
  on S.INSTITUTION = G.INSTITUTION_CD
 and S.ACAD_PROG = G.ACAD_PROG_CD
 and S.SRC_SYS_ID = G.SRC_SYS_ID
 and G.EFFDT_ORDER = 1
left outer join UM_D_ACAD_PLAN P
  on S.INSTITUTION = P.INSTITUTION_CD
 and S.ACAD_PLAN = P.ACAD_PLAN_CD
 and S.SRC_SYS_ID = P.SRC_SYS_ID
 and P.EFFDT_ORDER = 1
left outer join STAT T1
  on S.INSTITUTION = T1.INSTITUTION
 and S.ACAD_CAREER = T1.ACAD_CAREER
 and S.SFA_SAP_STATUS = T1.SFA_SAP_STATUS
 and S.SRC_SYS_ID = T1.SRC_SYS_ID
 and T1.STAT_ORDER = 1
left outer join STAT T2
  on S.INSTITUTION = T2.INSTITUTION
 and S.ACAD_CAREER = T2.ACAD_CAREER
 and S.SFA_SAP_STAT_CALC = T2.SFA_SAP_STATUS
 and S.SRC_SYS_ID = T2.SRC_SYS_ID
 and T2.STAT_ORDER = 1
where S.DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_STDNT_SFA_SAP rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_SFA_SAP',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_STDNT_SFA_SAP',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_STDNT_SFA_SAP enable constraint PK_UM_F_FA_STDNT_SFA_SAP';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_STDNT_SFA_SAP');

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

END UM_F_FA_STDNT_SFA_SAP_P;
/
