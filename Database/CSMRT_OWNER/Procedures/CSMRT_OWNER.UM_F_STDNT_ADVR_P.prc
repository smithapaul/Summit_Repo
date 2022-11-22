DROP PROCEDURE CSMRT_OWNER.UM_F_STDNT_ADVR_P
/

--
-- UM_F_STDNT_ADVR_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_STDNT_ADVR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_STDNT_ADVR
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_STDNT_ADVR';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_STDNT_ADVR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_STDNT_ADVR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ADVR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_STDNT_ADVR');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ADVR disable constraint PK_UM_F_STDNT_ADVR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_STDNT_ADVR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_STDNT_ADVR';				
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_F_STDNT_ADVR 
with X as (
select /*+ parallel(8) inline no_merge */
     FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID,
     XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN,
     row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                            order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
from CSSTG_OWNER.PSXLATITEM
where DATA_ORIGIN <> 'D'),
     Q2 as (
select /*+ parallel(8) inline no_merge USE_HASH(Q1 P G L T) */ 
     T.TERM_SID,
     Q1.INSTITUTION INSTITUTION_CD,
     Q1.ACAD_CAREER ACAD_CAR_CD,
     Q1.EMPLID PERSON_ID,
     Q1.ACAD_PROG ACAD_PROG_CD,
     Q1.ACAD_PLAN ACAD_PLAN_CD,
     Q1.ADVISOR_ROLE,
     Q1.STDNT_ADVISOR_NBR,
     Q1.SRC_SYS_ID,
     Q1.EFFDT,
     nvl(P.PERSON_SID,2147483646) PERSON_SID,
     nvl(G.ACAD_PROG_SID,2147483646) ACAD_PROG_SID,
     nvl(L.ACAD_PLAN_SID,2147483646) ACAD_PLAN_SID,
     Q1.ADVISOR_ID,
     Q1.APPROVE_ENRLMT,
     Q1.APPROVE_GRAD,
     Q1.GRAD_APPROVED,
     Q1.COMMITTEE_ID,
     Q1.COMM_PERS_CD,
     dense_rank() over (partition by Q1.INSTITUTION, T.TERM_CD, Q1.EMPLID, Q1.SRC_SYS_ID
                            order by Q1.EFFDT DESC) ADV_TERM_ORDER
from CSSTG_OWNER.PS_STDNT_ADVR_HIST Q1    -- PK --> EMPLID, INSTITUTION, EFFDT, ADVISOR_ROLE, STDNT_ADVISOR_NBR, SRC_SYS_ID
join PS_D_PERSON P
  on Q1.EMPLID = P.PERSON_ID
 and Q1.SRC_SYS_ID = P.SRC_SYS_ID
join UM_D_ACAD_PROG G
  on Q1.INSTITUTION = G.INSTITUTION_CD
 and Q1.ACAD_PROG = G.ACAD_PROG_CD
 and Q1.SRC_SYS_ID = G.SRC_SYS_ID
 and G.EFFDT_ORDER = 1
join UM_D_ACAD_PLAN L
  on Q1.INSTITUTION = L.INSTITUTION_CD
 and Q1.ACAD_PLAN = L.ACAD_PLAN_CD
 and Q1.SRC_SYS_ID = L.SRC_SYS_ID
 and L.EFFDT_ORDER = 1
join PS_D_TERM T
  on Q1.INSTITUTION = T.INSTITUTION_CD
 and Q1.ACAD_CAREER = T.ACAD_CAR_CD
 and Q1.SRC_SYS_ID = T.SRC_SYS_ID
 and Q1.EFFDT <= T.TERM_END_DT
where Q1.DATA_ORIGIN <> 'D')
,
     Q3 as (
select /*+ parallel(8) inline no_merge USE_HASH(S) */ distinct  
     S.TERM_SID,
     S.PERSON_SID,
     S.STDNT_CAR_NUM,
     S.ACAD_PLAN_SID,
     S.ACAD_SPLAN_SID,
     S.SRC_SYS_ID,
     S.INSTITUTION_CD,
     S.ACAD_CAR_CD,
     S.TERM_CD,
     S.PERSON_ID,
     S.ACAD_PROG_CD,
     S.ACAD_PLAN_CD,
     S.ACAD_SPLAN_CD,
     S.INSTITUTION_SID,
     S.ACAD_CAR_SID,
     S.ACAD_PROG_SID,
     S.TERM_BEGIN_DT,  
     S.TERM_END_DT,      
     0 SRC   
from UM_F_STDNT_ACAD_STRUCT S     
union all
select /*+ parallel(8) inline no_merge USE_HASH(S) */ distinct  
     S.TERM_SID,
     S.PERSON_SID,
     S.STDNT_CAR_NUM,
     S.ACAD_PLAN_SID,
     S.ACAD_SPLAN_SID,
     S.SRC_SYS_ID,
     S.INSTITUTION_CD,
     S.ACAD_CAR_CD,
     S.TERM_CD,
     S.PERSON_ID,
     S.ACAD_PROG_CD,
     S.ACAD_PLAN_CD,
     S.ACAD_SPLAN_CD,
     S.INSTITUTION_SID,
     S.ACAD_CAR_SID,
     S.ACAD_PROG_SID,
     TERM_BEGIN_DT,  
     TERM_END_DT,      
     1 SRC   
from CSMRT_OWNER.UM_F_ADM_APPL_ACAD_STRUCT S     
    ),
     Q4 as (
select /*+ parallel(8) inline  no_merge USE_HASH(Q3 Q2) */
     Q3.TERM_SID,
     Q3.PERSON_SID,
     Q3.STDNT_CAR_NUM,
     Q3.ACAD_PLAN_SID,
     Q3.ACAD_SPLAN_SID,
     nvl(Q2.ADVISOR_ROLE,'-') ADVISOR_ROLE,
     nvl(Q2.STDNT_ADVISOR_NBR,0) STDNT_ADVISOR_NBR,
     Q3.SRC_SYS_ID,
     Q3.INSTITUTION_CD,
     Q3.ACAD_CAR_CD,
     Q3.TERM_CD,
     Q3.PERSON_ID,
     Q3.ACAD_PROG_CD,
     Q3.ACAD_PLAN_CD,
     Q3.ACAD_SPLAN_CD,
     Q2.EFFDT,
     Q3.INSTITUTION_SID,
     Q3.ACAD_CAR_SID,
     Q3.ACAD_PROG_SID,
     nvl(Q2.ADVISOR_ID,'-') ADVISOR_ID,
     nvl(Q2.APPROVE_ENRLMT,'-') APPROVE_ENRLMT,
     nvl(Q2.APPROVE_GRAD,'-') APPROVE_GRAD,
     nvl(Q2.GRAD_APPROVED,'-') GRAD_APPROVED,
     nvl(Q2.COMMITTEE_ID,'-') COMMITTEE_ID,
     nvl(Q2.COMM_PERS_CD,'-') COMM_PERS_CD,
     Q3.TERM_BEGIN_DT,   
     Q3.TERM_END_DT,     
     row_number() over (partition by Q3.TERM_SID, Q3.PERSON_SID, Q3.STDNT_CAR_NUM, Q3.ACAD_PLAN_SID, Q3.ACAD_SPLAN_SID, Q2.ADVISOR_ROLE, Q2.STDNT_ADVISOR_NBR, Q3.SRC_SYS_ID
                            order by Q3.SRC) ADV_ORDER
from  Q3  
left outer join Q2
  on Q2.TERM_SID = Q3.TERM_SID
 and Q2.PERSON_SID = Q3.PERSON_SID
 and Q2.ACAD_PROG_SID = Q3.ACAD_PROG_SID
 and Q2.ACAD_PLAN_SID = Q3.ACAD_PLAN_SID
 and Q2.SRC_SYS_ID = Q3.SRC_SYS_ID
 and Q2.ADV_TERM_ORDER = 1                
   )                         
select /*+ parallel(8) inline USE_HASH(Q4 X P2) */
     ADV.TERM_SID,
     ADV.PERSON_SID,
     ADV.STDNT_CAR_NUM,
     ADV.ACAD_PLAN_SID,
     ADV.ACAD_SPLAN_SID,
     ADV.ADVISOR_ROLE,
     ADV.STDNT_ADVISOR_NBR,
     ADV.SRC_SYS_ID,
     ADV.INSTITUTION_CD,
     ADV.ACAD_CAR_CD,
     ADV.TERM_CD,
     ADV.PERSON_ID,
     ADV.ACAD_PROG_CD,
     ADV.ACAD_PLAN_CD,
     ADV.ACAD_SPLAN_CD,
     ADV.EFFDT,
     nvl(X1.XLATSHORTNAME,'-') ADVISOR_ROLE_SD,
     nvl(X1.XLATLONGNAME,'-') ADVISOR_ROLE_LD,
     ADV.INSTITUTION_SID,
     ADV.ACAD_CAR_SID,
     ADV.ACAD_PROG_SID,
     nvl(P2.PERSON_SID,2147483646) STUDENT_ADVISOR_SID,
     P2.PERSON_NM STUDENT_ADVISOR_NM,
     dense_rank() over (partition by ADV.INSTITUTION_CD, ADV.ACAD_CAR_CD, ADV.TERM_CD, ADV.PERSON_ID, ADV.ACAD_PLAN_CD, ADV.SRC_SYS_ID
                            order by decode(ADVISOR_ROLE,'FAC',1,'ADVR',2,'PROF',3,'MADV',4,'HONR',5,'ATHL',6,'PROG',7,9),
                                     (case when upper(P2.PERSON_NM) like 'DEPARTMENT%' then 999999
                                           when upper(P2.PERSON_NM) like 'ADVISING%' then 999999
                                           when upper(P2.PERSON_NM) like 'PROGRAM%' then 999999
                                      else STDNT_ADVISOR_NBR end)) STUDENT_ADVISOR_ORDER,
     nvl(ADV.APPROVE_ENRLMT,'-') APPROVE_ENRLMT,
     nvl(ADV.APPROVE_GRAD,'-') APPROVE_GRAD,
     nvl(ADV.GRAD_APPROVED,'-') GRAD_APPROVED,
     nvl(ADV.COMMITTEE_ID,'-') COMMITTEE_ID,
     nvl(ADV.COMM_PERS_CD,'-') COMM_PERS_CD,
     ADV.TERM_BEGIN_DT,
     ADV.TERM_END_DT,
     'S' DATA_ORIGIN,
     SYSDATE CREATED_EW_DTTM,
     SYSDATE LASTUPD_EW_DTTM
from Q4 ADV
left outer join X X1
  on ADV.ADVISOR_ROLE = X1.FIELDVALUE
 and ADV.SRC_SYS_ID = X1.SRC_SYS_ID
 and X1.FIELDNAME = 'ADVISOR_ROLE'
 and X1.X_ORDER = 1
left outer join PS_D_PERSON P2
  on ADV.ADVISOR_ID = P2.PERSON_ID
 and ADV.SRC_SYS_ID = P2.SRC_SYS_ID
  where  ADV.ADV_ORDER = 1;
  
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_STDNT_ADVR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_STDNT_ADVR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_STDNT_ADVR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_STDNT_ADVR enable constraint PK_UM_F_STDNT_ADVR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_STDNT_ADVR');

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

END UM_F_STDNT_ADVR_P;
/
