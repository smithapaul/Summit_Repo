DROP PROCEDURE CSMRT_OWNER.UM_D_ACAD_SPLAN_P
/

--
-- UM_D_ACAD_SPLAN_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_ACAD_SPLAN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- UM_D_ACAD_SPLAN / UM_D_ACAD_SPLAN_VW 
--Loads target table       -- UM_D_ACAD_SPLAN
--UM_D_ACAD_SPLAN          -- Dependent on PS_D_INSTITUTION, UM_D_ACAD_PLAN
-- V01 4/22/2018           -- srikanth ,pabbu converted to proc from sql
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_ACAD_SPLAN';
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

strMessage01    := 'Merging data into CSSTG_OWNER.UM_D_ACAD_SPLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UM_D_ACAD_SPLAN';

merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_ACAD_SPLAN T  
using (
  with XL as (  
select /*+ inline */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       CIP as (
select CIP_CODE, EFFDT, SRC_SYS_ID,
       decode(max(EFFDT) over (partition by CIP_CODE, SRC_SYS_ID
                                   order by EFFDT 
                               rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),EFFDT) EFFDT_START,   
       nvl(min(EFFDT-1) over (partition by CIP_CODE, SRC_SYS_ID
                                  order by EFFDT
                              rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,   
       DESCR, SEV_VALID_CIP_CD, UM_STEM
  from CSSTG_OWNER.PS_CIP_CODE_TBL
 where DATA_ORIGIN <> 'D'),
       D2 as (
select distinct
       ACAD_SPLAN_SID, INSTITUTION_CD, ACAD_PLAN_CD, ACAD_SPLAN_CD, SRC_SYS_ID
  from CSMRT_OWNER.UM_D_ACAD_SPLAN),   
       N1 as (
select distinct 
       INSTITUTION, ACAD_PLAN, ACAD_SUB_PLAN, SRC_SYS_ID
  from CSSTG_OWNER.PS_ACAD_SUBPLN_TBL
 where DATA_ORIGIN <> 'D'
 minus
select INSTITUTION_CD, ACAD_PLAN_CD, ACAD_SPLAN_CD, SRC_SYS_ID
  from CSMRT_OWNER.UM_D_ACAD_SPLAN),  
       N2 as (
select max(ACAD_SPLAN_SID) MAX_SID
  from CSMRT_OWNER.UM_D_ACAD_SPLAN  
 where ACAD_SPLAN_SID <> 2147483646),
       N3 as (
select N1.INSTITUTION, N1.ACAD_PLAN, N1.ACAD_SUB_PLAN, N1.SRC_SYS_ID, 
       nvl(N2.MAX_SID,0) + row_number() over (partition by 1 
                                                  order by N1.INSTITUTION, N1.ACAD_PLAN, N1.ACAD_SUB_PLAN, N1.SRC_SYS_ID nulls first) NEW_SID
  from N1, N2),
       S as (
select
SPLAN.EFFDT,
SPLAN.INSTITUTION INSTITUTION_CD,
SPLAN.ACAD_PLAN ACAD_PLAN_CD,
SPLAN.ACAD_SUB_PLAN ACAD_SPLAN_CD,
SPLAN.SRC_SYS_ID,
decode(max(SPLAN.EFFDT) over (partition by SPLAN.INSTITUTION, SPLAN.ACAD_PLAN, SPLAN.ACAD_SUB_PLAN, SPLAN.SRC_SYS_ID
                                  order by SPLAN.EFFDT 
                            rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),SPLAN.EFFDT) EFFDT_START,      -- Added  
nvl(min(SPLAN.EFFDT-1) over (partition by SPLAN.INSTITUTION, SPLAN.ACAD_PLAN, SPLAN.ACAD_SUB_PLAN, SPLAN.SRC_SYS_ID
                                order by SPLAN.EFFDT
                            rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,      -- Added  
row_number() over (partition by SPLAN.INSTITUTION, SPLAN.ACAD_PLAN, SPLAN.ACAD_SUB_PLAN, SPLAN.SRC_SYS_ID
                       order by SPLAN.EFFDT desc) EFFDT_ORDER,                                                         -- Added 
SPLAN.EFF_STATUS EFF_STAT_CD,
SPLAN.DESCRSHORT ACAD_SPLAN_SD,
SPLAN.DESCR ACAD_SPLAN_LD,
SPLAN.ACAD_SUB_PLAN || ' (' || SPLAN.DESCR || ')' ACAD_SPLAN_CD_DESC,
nvl(P.ACAD_PLAN_SID, 2147483646) ACAD_PLAN_SID, 
nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
SPLAN.ACAD_SUBPLAN_TYPE ACAD_SPLAN_TYPE_CD,
nvl(X1.XLATSHORTNAME,'-') ACAD_SPLAN_TYPE_SD,
nvl(X1.XLATLONGNAME,'-') ACAD_SPLAN_TYPE_LD,
SPLAN.ACAD_SUBPLAN_TYPE || ' (' || nvl(X1.XLATLONGNAME,'-') || ')' ACAD_SPLAN_TYPE_CD_DESC,
SPLAN.CIP_CODE CIP_CD,
nvl(CIP.DESCR,'-') CIP_LD,
SPLAN.DIPLOMA_DESCR DIPLOMA_LD,
SPLAN.DIPLOMA_PRINT_FL DIPLOMA_PRINT_FLG,
SPLAN.EVALUATE_SUBPLAN EVALUATE_SPLAN_FLG,
nvl(CIP.SEV_VALID_CIP_CD,'-') SEV_VALID_CIP_FLG,
SPLAN.SUBPLN_REQTRM_DFLT SPLAN_REQTRM_DFLT,
nvl(X2.XLATSHORTNAME,'-') SPLAN_REQTRM_DFLT_SD,
nvl(X2.XLATLONGNAME,'-') SPLAN_REQTRM_DFLT_LD,
SPLAN.TRNSCR_DESCR,
SPLAN.TRNSCR_PRINT_FL TRNSCR_PRINT_FLG,
nvl(CIP.UM_STEM,'-') UM_STEM_FLG
from CSSTG_OWNER.PS_ACAD_SUBPLN_TBL SPLAN
  left outer join CIP
    on SPLAN.CIP_CODE = CIP.CIP_CODE
   and SPLAN.SRC_SYS_ID = CIP.SRC_SYS_ID
   and SPLAN.EFFDT between CIP.EFFDT_START and CIP.EFFDT_END
  left outer join CSMRT_OWNER.UM_D_ACAD_PLAN P   
    on SPLAN.INSTITUTION = P.INSTITUTION_CD
   and SPLAN.ACAD_PLAN = P.ACAD_PLAN_CD
   and SPLAN.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
   and P.EFFDT_ORDER = 1
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I 
    on SPLAN.INSTITUTION = I.INSTITUTION_CD
   and SPLAN.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join XL X1
    on X1.FIELDNAME = 'ACAD_SUBPLAN_TYPE'
   and X1.FIELDVALUE = SPLAN.ACAD_SUBPLAN_TYPE 
   and X1.SRC_SYS_ID = SPLAN.SRC_SYS_ID
   and X1.X_ORDER = 1 
  left outer join XL X2
    on X2.FIELDNAME = 'SUBPLN_REQTRM_DFLT'
   and X2.FIELDVALUE = SPLAN.SUBPLN_REQTRM_DFLT 
   and X2.SRC_SYS_ID = SPLAN.SRC_SYS_ID
   and X2.X_ORDER = 1 
 where SPLAN.DATA_ORIGIN <> 'D')
select nvl(nvl(D.ACAD_SPLAN_SID, D2.ACAD_SPLAN_SID), N3.NEW_SID) ACAD_SPLAN_SID,
       nvl(D.EFFDT, S.EFFDT) EFFDT,  
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.ACAD_PLAN_CD, S.ACAD_PLAN_CD) ACAD_PLAN_CD, 
       nvl(D.ACAD_SPLAN_CD, S.ACAD_SPLAN_CD) ACAD_SPLAN_CD, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.EFFDT_START,S.EFFDT_START,D.EFFDT_START,S.EFFDT_START) EFFDT_START,
       decode(D.EFFDT_END,S.EFFDT_END,D.EFFDT_END,S.EFFDT_END) EFFDT_END,
       decode(D.EFFDT_ORDER,S.EFFDT_ORDER,D.EFFDT_ORDER,S.EFFDT_ORDER) EFFDT_ORDER,
       decode(D.EFF_STAT_CD,S.EFF_STAT_CD,D.EFF_STAT_CD,S.EFF_STAT_CD) EFF_STAT_CD,
       decode(D.ACAD_SPLAN_SD,S.ACAD_SPLAN_SD,D.ACAD_SPLAN_SD,S.ACAD_SPLAN_SD) ACAD_SPLAN_SD,
       decode(D.ACAD_SPLAN_LD,S.ACAD_SPLAN_LD,D.ACAD_SPLAN_LD,S.ACAD_SPLAN_LD) ACAD_SPLAN_LD,
       decode(D.ACAD_SPLAN_CD_DESC,S.ACAD_SPLAN_CD_DESC,D.ACAD_SPLAN_CD_DESC,S.ACAD_SPLAN_CD_DESC) ACAD_SPLAN_CD_DESC,
       decode(D.ACAD_PLAN_SID,S.ACAD_PLAN_SID,D.ACAD_PLAN_SID,S.ACAD_PLAN_SID) ACAD_PLAN_SID,
       decode(D.INSTITUTION_SID,S.INSTITUTION_SID,D.INSTITUTION_SID,S.INSTITUTION_SID) INSTITUTION_SID,
       decode(D.ACAD_SPLAN_TYPE_CD,S.ACAD_SPLAN_TYPE_CD,D.ACAD_SPLAN_TYPE_CD,S.ACAD_SPLAN_TYPE_CD) ACAD_SPLAN_TYPE_CD,
       decode(D.ACAD_SPLAN_TYPE_SD,S.ACAD_SPLAN_TYPE_SD,D.ACAD_SPLAN_TYPE_SD,S.ACAD_SPLAN_TYPE_SD) ACAD_SPLAN_TYPE_SD,
       decode(D.ACAD_SPLAN_TYPE_LD,S.ACAD_SPLAN_TYPE_LD,D.ACAD_SPLAN_TYPE_LD,S.ACAD_SPLAN_TYPE_LD) ACAD_SPLAN_TYPE_LD,
       decode(D.ACAD_SPLAN_TYPE_CD_DESC,S.ACAD_SPLAN_TYPE_CD_DESC,D.ACAD_SPLAN_TYPE_CD_DESC,S.ACAD_SPLAN_TYPE_CD_DESC) ACAD_SPLAN_TYPE_CD_DESC,
       decode(D.CIP_CD,S.CIP_CD,D.CIP_CD,S.CIP_CD) CIP_CD,
       decode(D.CIP_LD,S.CIP_LD,D.CIP_LD,S.CIP_LD) CIP_LD,
       decode(D.DIPLOMA_LD,S.DIPLOMA_LD,D.DIPLOMA_LD,S.DIPLOMA_LD) DIPLOMA_LD,
       decode(D.DIPLOMA_PRINT_FLG,S.DIPLOMA_PRINT_FLG,D.DIPLOMA_PRINT_FLG,S.DIPLOMA_PRINT_FLG) DIPLOMA_PRINT_FLG,
       decode(D.EVALUATE_SPLAN_FLG,S.EVALUATE_SPLAN_FLG,D.EVALUATE_SPLAN_FLG,S.EVALUATE_SPLAN_FLG) EVALUATE_SPLAN_FLG,
       decode(D.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG,D.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG) SEV_VALID_CIP_FLG,
       decode(D.SPLAN_REQTRM_DFLT,S.SPLAN_REQTRM_DFLT,D.SPLAN_REQTRM_DFLT,S.SPLAN_REQTRM_DFLT) SPLAN_REQTRM_DFLT,
       decode(D.SPLAN_REQTRM_DFLT_SD,S.SPLAN_REQTRM_DFLT_SD,D.SPLAN_REQTRM_DFLT_SD,S.SPLAN_REQTRM_DFLT_SD) SPLAN_REQTRM_DFLT_SD,
       decode(D.SPLAN_REQTRM_DFLT_LD,S.SPLAN_REQTRM_DFLT_LD,D.SPLAN_REQTRM_DFLT_LD,S.SPLAN_REQTRM_DFLT_LD) SPLAN_REQTRM_DFLT_LD,
       decode(D.TRNSCR_DESCR,S.TRNSCR_DESCR,D.TRNSCR_DESCR,S.TRNSCR_DESCR) TRNSCR_DESCR,
       decode(D.TRNSCR_PRINT_FLG,S.TRNSCR_PRINT_FLG,D.TRNSCR_PRINT_FLG,S.TRNSCR_PRINT_FLG) TRNSCR_PRINT_FLG,
       decode(D.UM_STEM_FLG,S.UM_STEM_FLG,D.UM_STEM_FLG,S.UM_STEM_FLG) UM_STEM_FLG,
       decode(D.DATA_ORIGIN,'S',D.DATA_ORIGIN,'S') DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S
  left outer join CSMRT_OWNER.UM_D_ACAD_SPLAN D  
    on D.EFFDT = S.EFFDT 
   and D.INSTITUTION_CD = S.INSTITUTION_CD
   and D.ACAD_PLAN_CD = S.ACAD_PLAN_CD
   and D.ACAD_SPLAN_CD = S.ACAD_SPLAN_CD
   and D.SRC_SYS_ID = S.SRC_SYS_ID
  left outer join D2
    on S.INSTITUTION_CD = D2.INSTITUTION_CD 
   and S.ACAD_PLAN_CD = D2.ACAD_PLAN_CD
   and S.ACAD_SPLAN_CD = D2.ACAD_SPLAN_CD
   and S.SRC_SYS_ID = D2.SRC_SYS_ID 
  left outer join N3
    on S.INSTITUTION_CD = N3.INSTITUTION 
   and S.ACAD_PLAN_CD = N3.ACAD_PLAN
   and S.ACAD_SPLAN_CD = N3.ACAD_SUB_PLAN
   and S.SRC_SYS_ID = N3.SRC_SYS_ID 
) S 
    on (T.EFFDT = S.EFFDT
   and  T.INSTITUTION_CD = S.INSTITUTION_CD 
   and  T.ACAD_PLAN_CD = S.ACAD_PLAN_CD 
   and  T.ACAD_SPLAN_CD = S.ACAD_SPLAN_CD 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)
  when matched then update set 
       T.EFFDT_START = S.EFFDT_START,
       T.EFFDT_END = S.EFFDT_END,
       T.EFFDT_ORDER = S.EFFDT_ORDER,
       T.EFF_STAT_CD = S.EFF_STAT_CD,
       T.ACAD_SPLAN_SD = S.ACAD_SPLAN_SD,
       T.ACAD_SPLAN_LD = S.ACAD_SPLAN_LD,
       T.ACAD_SPLAN_CD_DESC = S.ACAD_SPLAN_CD_DESC,
       T.ACAD_PLAN_SID = S.ACAD_PLAN_SID,
       T.INSTITUTION_SID = S.INSTITUTION_SID,
       T.ACAD_SPLAN_TYPE_CD = S.ACAD_SPLAN_TYPE_CD,
       T.ACAD_SPLAN_TYPE_SD = S.ACAD_SPLAN_TYPE_SD,
       T.ACAD_SPLAN_TYPE_LD = S.ACAD_SPLAN_TYPE_LD,
       T.ACAD_SPLAN_TYPE_CD_DESC = S.ACAD_SPLAN_TYPE_CD_DESC,
       T.CIP_CD = S.CIP_CD,
       T.CIP_LD = S.CIP_LD,
       T.DIPLOMA_LD = S.DIPLOMA_LD,
       T.DIPLOMA_PRINT_FLG = S.DIPLOMA_PRINT_FLG,
       T.EVALUATE_SPLAN_FLG = S.EVALUATE_SPLAN_FLG,
       T.SEV_VALID_CIP_FLG = S.SEV_VALID_CIP_FLG,
       T.SPLAN_REQTRM_DFLT = S.SPLAN_REQTRM_DFLT,
       T.SPLAN_REQTRM_DFLT_SD = S.SPLAN_REQTRM_DFLT_SD,
       T.SPLAN_REQTRM_DFLT_LD = S.SPLAN_REQTRM_DFLT_LD,
       T.TRNSCR_DESCR = S.TRNSCR_DESCR,
       T.TRNSCR_PRINT_FLG = S.TRNSCR_PRINT_FLG,
       T.UM_STEM_FLG = S.UM_STEM_FLG,
       T.DATA_ORIGIN = S.DATA_ORIGIN,
       T.LASTUPD_EW_DTTM = S.LASTUPD_EW_DTTM
 where 
       decode(T.EFFDT_START,S.EFFDT_START,0,1) = 1 or 
       decode(T.EFFDT_END,S.EFFDT_END,0,1) = 1 or 
       decode(T.EFFDT_ORDER,S.EFFDT_ORDER,0,1) = 1 or 
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or 
       decode(T.ACAD_SPLAN_SD,S.ACAD_SPLAN_SD,0,1) = 1 or 
       decode(T.ACAD_SPLAN_LD,S.ACAD_SPLAN_LD,0,1) = 1 or 
       decode(T.ACAD_SPLAN_CD_DESC,S.ACAD_SPLAN_CD_DESC,0,1) = 1 or 
       decode(T.ACAD_PLAN_SID,S.ACAD_PLAN_SID,0,1) = 1 or 
       decode(T.INSTITUTION_SID,S.INSTITUTION_SID,0,1) = 1 or 
       decode(T.ACAD_SPLAN_TYPE_CD,S.ACAD_SPLAN_TYPE_CD,0,1) = 1 or 
       decode(T.ACAD_SPLAN_TYPE_SD,S.ACAD_SPLAN_TYPE_SD,0,1) = 1 or 
       decode(T.ACAD_SPLAN_TYPE_LD,S.ACAD_SPLAN_TYPE_LD,0,1) = 1 or 
       decode(T.ACAD_SPLAN_TYPE_CD_DESC,S.ACAD_SPLAN_TYPE_CD_DESC,0,1) = 1 or 
       decode(T.CIP_CD,S.CIP_CD,0,1) = 1 or 
       decode(T.CIP_LD,S.CIP_LD,0,1) = 1 or 
       decode(T.DIPLOMA_LD,S.DIPLOMA_LD,0,1) = 1 or 
       decode(T.DIPLOMA_PRINT_FLG,S.DIPLOMA_PRINT_FLG,0,1) = 1 or 
       decode(T.EVALUATE_SPLAN_FLG,S.EVALUATE_SPLAN_FLG,0,1) = 1 or 
       decode(T.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG,0,1) = 1 or 
       decode(T.SPLAN_REQTRM_DFLT,S.SPLAN_REQTRM_DFLT,0,1) = 1 or 
       decode(T.SPLAN_REQTRM_DFLT_SD,S.SPLAN_REQTRM_DFLT_SD,0,1) = 1 or 
       decode(T.SPLAN_REQTRM_DFLT_LD,S.SPLAN_REQTRM_DFLT_LD,0,1) = 1 or 
       decode(T.TRNSCR_DESCR,S.TRNSCR_DESCR,0,1) = 1 or 
       decode(T.TRNSCR_PRINT_FLG,S.TRNSCR_PRINT_FLG,0,1) = 1 or 
       decode(T.UM_STEM_FLG,S.UM_STEM_FLG,0,1) = 1 or
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
  when not matched then
insert (
       T.ACAD_SPLAN_SID,
       T.EFFDT,
       T.INSTITUTION_CD,
       T.ACAD_PLAN_CD,
       T.ACAD_SPLAN_CD,
       T.SRC_SYS_ID,
       T.EFFDT_START,
       T.EFFDT_END,
       T.EFFDT_ORDER,
       T.EFF_STAT_CD,
       T.ACAD_SPLAN_SD,
       T.ACAD_SPLAN_LD,
       T.ACAD_SPLAN_CD_DESC,
       T.ACAD_PLAN_SID,
       T.INSTITUTION_SID,
       T.ACAD_SPLAN_TYPE_CD,
       T.ACAD_SPLAN_TYPE_SD,
       T.ACAD_SPLAN_TYPE_LD,
       T.ACAD_SPLAN_TYPE_CD_DESC,
       T.CIP_CD,
       T.CIP_LD,
       T.DIPLOMA_LD,
       T.DIPLOMA_PRINT_FLG,
       T.EVALUATE_SPLAN_FLG,
       T.SEV_VALID_CIP_FLG,
       T.SPLAN_REQTRM_DFLT,
       T.SPLAN_REQTRM_DFLT_SD,
       T.SPLAN_REQTRM_DFLT_LD,
       T.TRNSCR_DESCR,
       T.TRNSCR_PRINT_FLG,
       T.UM_STEM_FLG,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values (
       S.ACAD_SPLAN_SID,
       S.EFFDT,
       S.INSTITUTION_CD,
       S.ACAD_PLAN_CD,
       S.ACAD_SPLAN_CD,
       S.SRC_SYS_ID,
       S.EFFDT_START,
       S.EFFDT_END,
       S.EFFDT_ORDER,
       S.EFF_STAT_CD,
       S.ACAD_SPLAN_SD,
       S.ACAD_SPLAN_LD,
       S.ACAD_SPLAN_CD_DESC,
       S.ACAD_PLAN_SID,
       S.INSTITUTION_SID,
       S.ACAD_SPLAN_TYPE_CD,
       S.ACAD_SPLAN_TYPE_SD,
       S.ACAD_SPLAN_TYPE_LD,
       S.ACAD_SPLAN_TYPE_CD_DESC,
       S.CIP_CD,
       S.CIP_LD,
       S.DIPLOMA_LD,
       S.DIPLOMA_PRINT_FLG,
       S.EVALUATE_SPLAN_FLG,
       S.SEV_VALID_CIP_FLG,
       S.SPLAN_REQTRM_DFLT,
       S.SPLAN_REQTRM_DFLT_SD,
       S.SPLAN_REQTRM_DFLT_LD,
       S.TRNSCR_DESCR,
       S.TRNSCR_PRINT_FLG,
       S.UM_STEM_FLG,
       S.DATA_ORIGIN,
       S.CREATED_EW_DTTM,
       S.LASTUPD_EW_DTTM)
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.UM_D_ACAD_SPLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.UM_D_ACAD_SPLAN';
update CSMRT_OWNER.UM_D_ACAD_SPLAN T   
   set EFFDT_START = '31-DEC-9999',
       EFFDT_ORDER = 9,
       DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ACAD_SPLAN_SID <> 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_ACAD_SUBPLN_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.ACAD_PLAN_CD = S.ACAD_PLAN
                      and T.ACAD_SPLAN_CD = S.ACAD_SUB_PLAN
                      and T.EFFDT = S.EFFDT
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_ACAD_SPLAN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_ACAD_SPLAN',
                i_Action            => 'UPDATE',
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

END UM_D_ACAD_SPLAN_P;
/
