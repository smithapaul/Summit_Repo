CREATE OR REPLACE PROCEDURE             "UM_D_ACAD_PLAN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- UM_D_ACAD_PLAN / UM_D_ACAD_PLAN_VW
--Loads target table       -- UM_D_ACAD_PLAN
--UM_D_ACAD_PLAN           -- Dependent on PS_D_INSTITUTION, PS_D_ACAD_CAR, UM_D_ACAD_PROG, PS_D_ACAD_ORG, PS_D_DEG
-- V01 4/22/2018           -- srikanth ,pabbu converted to proc from sql
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_ACAD_PLAN';
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

strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_ACAD_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_ACAD_PLAN';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_ACAD_PLAN T  
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
       ACAD_PLAN_SID, INSTITUTION_CD, ACAD_PLAN_CD, SRC_SYS_ID
  from CSMRT_OWNER.UM_D_ACAD_PLAN),   
       N1 as (
select distinct 
       INSTITUTION, ACAD_PLAN, SRC_SYS_ID
  from CSSTG_OWNER.PS_ACAD_PLAN_TBL
 where DATA_ORIGIN <> 'D'
 minus
select INSTITUTION_CD, ACAD_PLAN_CD, SRC_SYS_ID
  from CSMRT_OWNER.UM_D_ACAD_PLAN  
),
       N2 as (
select max(ACAD_PLAN_SID) MAX_SID
  from CSMRT_OWNER.UM_D_ACAD_PLAN  
 where ACAD_PLAN_SID <> 2147483646),
       N3 as (
select N1.INSTITUTION, N1.ACAD_PLAN, N1.SRC_SYS_ID, 
       nvl(N2.MAX_SID,0) + row_number() over (partition by 1 order by N1.INSTITUTION, N1.ACAD_PLAN, N1.SRC_SYS_ID nulls first) NEW_SID
  from N1, N2),
       S as (
select
PLAN.EFFDT,
PLAN.INSTITUTION INSTITUTION_CD,
PLAN.ACAD_PLAN ACAD_PLAN_CD,
PLAN.SRC_SYS_ID,
decode(max(PLAN.EFFDT) over (partition by PLAN.INSTITUTION, PLAN.ACAD_PLAN, PLAN.SRC_SYS_ID
                                  order by PLAN.EFFDT 
                            rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),PLAN.EFFDT) EFFDT_START,      -- Added  
nvl(min(PLAN.EFFDT-1) over (partition by PLAN.INSTITUTION, PLAN.ACAD_PLAN, PLAN.SRC_SYS_ID
                                order by PLAN.EFFDT
                            rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,      -- Added  
row_number() over (partition by PLAN.INSTITUTION, PLAN.ACAD_PLAN, PLAN.SRC_SYS_ID
                       order by PLAN.EFFDT desc) EFFDT_ORDER,                                                         -- Added 
PLAN.EFF_STATUS EFF_STAT_CD,
PLAN.DESCRSHORT ACAD_PLAN_SD,
PLAN.DESCR ACAD_PLAN_LD,
PLAN.ACAD_PLAN || ' (' || PLAN.DESCR || ')' ACAD_PLAN_CD_DESC,
nvl(C.ACAD_CAR_SID, 2147483646) ACAD_CAR_SID, 
nvl(G.ACAD_PROG_SID, 2147483646) ACAD_PROG_SID, 
nvl(E.DEG_SID, 2147483646) DEG_SID,
nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
PLAN.ACAD_PLAN_TYPE ACAD_PLAN_TYPE_CD,
nvl(X1.XLATSHORTNAME,'-') ACAD_PLAN_TYPE_SD,
nvl(X1.XLATLONGNAME,'-') ACAD_PLAN_TYPE_LD,
PLAN.ACAD_PLAN_TYPE || ' (' || nvl(X1.XLATLONGNAME,'-') || ')' ACAD_PLAN_TYPE_CD_DESC,
PLAN.CIP_CODE CIP_CD,
nvl(CIP.DESCR,'-') CIP_LD,
PLAN.DIPLOMA_DESCR,
PLAN.DIPLOMA_PRINT_FL DIPLOMA_PRINT_FLG,
CASE WHEN PLAN.ACAD_PLAN_TYPE IN ('CRT','OUC','PMC','OGC')
     THEN 'Certificate'
     WHEN PLAN.INSTITUTION = 'UMLOW' AND E.DEG_CD IN ('EDS', 'CAGS')
     THEN 'EDS/CAGS'
     WHEN PLAN.ACAD_PLAN_TYPE = 'MAJ' AND nvl(E.DEG_CD,'-') = '-'
     THEN 'Bachelors'
     ELSE nvl(E.EDU_LVL_SD,'-')
 END EDU_LVL_CTGRY,
PLAN.EVALUATE_PLAN EVALUATE_PLAN_FLG,
PLAN.PLN_REQTRM_DFLT PLAN_REQTRM_DFLT,
nvl(X2.XLATSHORTNAME,'-') PLAN_REQTRM_DFLT_SD,
nvl(X2.XLATLONGNAME,'-') PLAN_REQTRM_DFLT_LD,
PLAN.SAA_WHIF_DISP_ADVR SAA_WHIF_DISP_ADVR_FLG,
PLAN.SAA_WHIF_DISP_PREM SAA_WHIF_DISP_PREM_FLG,
PLAN.SAA_WHIF_DISP_STD SAA_WHIF_DISP_STD_FLG,
nvl(trim(PLAN.SSR_NSC_CRD_LVL),'-') SSR_NSC_CRD_LVL,
nvl(X3.XLATSHORTNAME,'-') SSR_NSC_CRD_LVL_SD,
nvl(X3.XLATLONGNAME,'-') SSR_NSC_CRD_LVL_LD,
PLAN.SSR_NSC_INCL_PLAN SSR_NSC_INCL_PLAN_FLG, 
nvl(trim(PLAN.SSR_PROG_LEN_TYPE),'-') SSR_PROG_LEN_TYPE,
nvl(X4.XLATSHORTNAME,'-') SSR_PROG_LEN_TYPE_SD,
nvl(X4.XLATLONGNAME,'-') SSR_PROG_LEN_TYPE_LD,
PLAN.SSR_PROG_LENGTH,
nvl(CIP.SEV_VALID_CIP_CD,'-') SEV_VALID_CIP_FLG,
PLAN.TRNSCR_DESCR,
PLAN.TRNSCR_PRINT_FL TRNSCR_PRINT_FLG,
nvl(CIP.UM_STEM,'-') UM_STEM_FLG
from CSSTG_OWNER.PS_ACAD_PLAN_TBL PLAN
  left outer join CIP
    on PLAN.CIP_CODE = CIP.CIP_CODE
   and PLAN.SRC_SYS_ID = CIP.SRC_SYS_ID
   and PLAN.EFFDT between CIP.EFFDT_START and CIP.EFFDT_END
  left outer join CSMRT_OWNER.PS_D_ACAD_CAR C       
    on PLAN.INSTITUTION = C.INSTITUTION_CD
   and PLAN.ACAD_CAREER = C.ACAD_CAR_CD
   and PLAN.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_ACAD_PROG G      
    on PLAN.INSTITUTION = G.INSTITUTION_CD
   and PLAN.ACAD_PROG = G.ACAD_PROG_CD
   and PLAN.SRC_SYS_ID = G.SRC_SYS_ID
   and G.DATA_ORIGIN <> 'D'
   and G.EFFDT_ORDER = 1
  left outer join CSMRT_OWNER.PS_D_DEG E           
    on PLAN.DEGREE = E.DEG_CD
   and PLAN.SRC_SYS_ID = E.SRC_SYS_ID
   and E.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I   
    on PLAN.INSTITUTION = I.INSTITUTION_CD
   and PLAN.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join XL X1
    on X1.FIELDNAME = 'ACAD_PLAN_TYPE'
   and X1.FIELDVALUE = PLAN.ACAD_PLAN_TYPE 
   and X1.SRC_SYS_ID = PLAN.SRC_SYS_ID
   and X1.X_ORDER = 1 
  left outer join XL X2
    on X2.FIELDNAME = 'PLN_REQTRM_DFLT'
   and X2.FIELDVALUE = PLAN.PLN_REQTRM_DFLT 
   and X2.SRC_SYS_ID = PLAN.SRC_SYS_ID
   and X2.X_ORDER = 1 
  left outer join XL X3
    on X3.FIELDNAME = 'SSR_NSC_CRD_LVL'
   and X3.FIELDVALUE = PLAN.SSR_NSC_CRD_LVL 
   and X3.SRC_SYS_ID = PLAN.SRC_SYS_ID
   and X3.X_ORDER = 1 
  left outer join XL X4
    on X4.FIELDNAME = 'SSR_PROG_LEN_TYPE'
   and X4.FIELDVALUE = PLAN.SSR_PROG_LEN_TYPE 
   and X4.SRC_SYS_ID = PLAN.SRC_SYS_ID
   and X4.X_ORDER = 1 
 where PLAN.DATA_ORIGIN <> 'D')
select nvl(nvl(D.ACAD_PLAN_SID, D2.ACAD_PLAN_SID), N3.NEW_SID) ACAD_PLAN_SID,
       nvl(D.EFFDT, S.EFFDT) EFFDT,  
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.ACAD_PLAN_CD, S.ACAD_PLAN_CD) ACAD_PLAN_CD, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.EFFDT_START,S.EFFDT_START,D.EFFDT_START,S.EFFDT_START) EFFDT_START,
       decode(D.EFFDT_END,S.EFFDT_END,D.EFFDT_END,S.EFFDT_END) EFFDT_END,
       decode(D.EFFDT_ORDER,S.EFFDT_ORDER,D.EFFDT_ORDER,S.EFFDT_ORDER) EFFDT_ORDER,
       decode(D.EFF_STAT_CD,S.EFF_STAT_CD,D.EFF_STAT_CD,S.EFF_STAT_CD) EFF_STAT_CD,
       decode(D.ACAD_PLAN_SD,S.ACAD_PLAN_SD,D.ACAD_PLAN_SD,S.ACAD_PLAN_SD) ACAD_PLAN_SD,
       decode(D.ACAD_PLAN_LD,S.ACAD_PLAN_LD,D.ACAD_PLAN_LD,S.ACAD_PLAN_LD) ACAD_PLAN_LD,
       decode(D.ACAD_PLAN_CD_DESC,S.ACAD_PLAN_CD_DESC,D.ACAD_PLAN_CD_DESC,S.ACAD_PLAN_CD_DESC) ACAD_PLAN_CD_DESC,
       decode(D.ACAD_CAR_SID,S.ACAD_CAR_SID,D.ACAD_CAR_SID,S.ACAD_CAR_SID) ACAD_CAR_SID,
       decode(D.ACAD_PROG_SID,S.ACAD_PROG_SID,D.ACAD_PROG_SID,S.ACAD_PROG_SID) ACAD_PROG_SID,
       decode(D.DEG_SID,S.DEG_SID,D.DEG_SID,S.DEG_SID) DEG_SID,
       decode(D.INSTITUTION_SID,S.INSTITUTION_SID,D.INSTITUTION_SID,S.INSTITUTION_SID) INSTITUTION_SID,
       decode(D.ACAD_PLAN_TYPE_CD,S.ACAD_PLAN_TYPE_CD,D.ACAD_PLAN_TYPE_CD,S.ACAD_PLAN_TYPE_CD) ACAD_PLAN_TYPE_CD,
       decode(D.ACAD_PLAN_TYPE_SD,S.ACAD_PLAN_TYPE_SD,D.ACAD_PLAN_TYPE_SD,S.ACAD_PLAN_TYPE_SD) ACAD_PLAN_TYPE_SD,
       decode(D.ACAD_PLAN_TYPE_LD,S.ACAD_PLAN_TYPE_LD,D.ACAD_PLAN_TYPE_LD,S.ACAD_PLAN_TYPE_LD) ACAD_PLAN_TYPE_LD,
       decode(D.ACAD_PLAN_TYPE_CD_DESC,S.ACAD_PLAN_TYPE_CD_DESC,D.ACAD_PLAN_TYPE_CD_DESC,S.ACAD_PLAN_TYPE_CD_DESC) ACAD_PLAN_TYPE_CD_DESC,
       decode(D.CIP_CD,S.CIP_CD,D.CIP_CD,S.CIP_CD) CIP_CD,
       decode(D.CIP_LD,S.CIP_LD,D.CIP_LD,S.CIP_LD) CIP_LD,
       decode(D.DIPLOMA_DESCR,S.DIPLOMA_DESCR,D.DIPLOMA_DESCR,S.DIPLOMA_DESCR) DIPLOMA_DESCR,
       decode(D.DIPLOMA_PRINT_FLG,S.DIPLOMA_PRINT_FLG,D.DIPLOMA_PRINT_FLG,S.DIPLOMA_PRINT_FLG) DIPLOMA_PRINT_FLG,
       decode(D.EDU_LVL_CTGRY,S.EDU_LVL_CTGRY,D.EDU_LVL_CTGRY,S.EDU_LVL_CTGRY) EDU_LVL_CTGRY,
       decode(D.EVALUATE_PLAN_FLG,S.EVALUATE_PLAN_FLG,D.EVALUATE_PLAN_FLG,S.EVALUATE_PLAN_FLG) EVALUATE_PLAN_FLG,
       decode(D.PLAN_REQTRM_DFLT,S.PLAN_REQTRM_DFLT,D.PLAN_REQTRM_DFLT,S.PLAN_REQTRM_DFLT) PLAN_REQTRM_DFLT,
       decode(D.PLAN_REQTRM_DFLT_SD,S.PLAN_REQTRM_DFLT_SD,D.PLAN_REQTRM_DFLT_SD,S.PLAN_REQTRM_DFLT_SD) PLAN_REQTRM_DFLT_SD,
       decode(D.PLAN_REQTRM_DFLT_LD,S.PLAN_REQTRM_DFLT_LD,D.PLAN_REQTRM_DFLT_LD,S.PLAN_REQTRM_DFLT_LD) PLAN_REQTRM_DFLT_LD,
       decode(D.SAA_WHIF_DISP_ADVR_FLG,S.SAA_WHIF_DISP_ADVR_FLG,D.SAA_WHIF_DISP_ADVR_FLG,S.SAA_WHIF_DISP_ADVR_FLG) SAA_WHIF_DISP_ADVR_FLG,
       decode(D.SAA_WHIF_DISP_PREM_FLG,S.SAA_WHIF_DISP_PREM_FLG,D.SAA_WHIF_DISP_PREM_FLG,S.SAA_WHIF_DISP_PREM_FLG) SAA_WHIF_DISP_PREM_FLG,
       decode(D.SAA_WHIF_DISP_STD_FLG,S.SAA_WHIF_DISP_STD_FLG,D.SAA_WHIF_DISP_STD_FLG,S.SAA_WHIF_DISP_STD_FLG) SAA_WHIF_DISP_STD_FLG,
       decode(D.SSR_NSC_CRD_LVL,S.SSR_NSC_CRD_LVL,D.SSR_NSC_CRD_LVL,S.SSR_NSC_CRD_LVL) SSR_NSC_CRD_LVL,
       decode(D.SSR_NSC_CRD_LVL_SD,S.SSR_NSC_CRD_LVL_SD,D.SSR_NSC_CRD_LVL_SD,S.SSR_NSC_CRD_LVL_SD) SSR_NSC_CRD_LVL_SD,
       decode(D.SSR_NSC_CRD_LVL_LD,S.SSR_NSC_CRD_LVL_LD,D.SSR_NSC_CRD_LVL_LD,S.SSR_NSC_CRD_LVL_LD) SSR_NSC_CRD_LVL_LD,
       decode(D.SSR_NSC_INCL_PLAN_FLG,S.SSR_NSC_INCL_PLAN_FLG,D.SSR_NSC_INCL_PLAN_FLG,S.SSR_NSC_INCL_PLAN_FLG) SSR_NSC_INCL_PLAN_FLG,
       decode(D.SSR_PROG_LEN_TYPE,S.SSR_PROG_LEN_TYPE,D.SSR_PROG_LEN_TYPE,S.SSR_PROG_LEN_TYPE) SSR_PROG_LEN_TYPE,
       decode(D.SSR_PROG_LEN_TYPE_SD,S.SSR_PROG_LEN_TYPE_SD,D.SSR_PROG_LEN_TYPE_SD,S.SSR_PROG_LEN_TYPE_SD) SSR_PROG_LEN_TYPE_SD,
       decode(D.SSR_PROG_LEN_TYPE_LD,S.SSR_PROG_LEN_TYPE_LD,D.SSR_PROG_LEN_TYPE_LD,S.SSR_PROG_LEN_TYPE_LD) SSR_PROG_LEN_TYPE_LD,
       decode(D.SSR_PROG_LENGTH,S.SSR_PROG_LENGTH,D.SSR_PROG_LENGTH,S.SSR_PROG_LENGTH) SSR_PROG_LENGTH,
       decode(D.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG,D.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG) SEV_VALID_CIP_FLG,
       decode(D.TRNSCR_DESCR,S.TRNSCR_DESCR,D.TRNSCR_DESCR,S.TRNSCR_DESCR) TRNSCR_DESCR,
       decode(D.TRNSCR_PRINT_FLG,S.TRNSCR_PRINT_FLG,D.TRNSCR_PRINT_FLG,S.TRNSCR_PRINT_FLG) TRNSCR_PRINT_FLG,
       decode(D.UM_STEM_FLG,S.UM_STEM_FLG,D.UM_STEM_FLG,S.UM_STEM_FLG) UM_STEM_FLG,
       decode(D.DATA_ORIGIN,'S',D.DATA_ORIGIN,'S') DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM 
  from S
  left outer join CSMRT_OWNER.UM_D_ACAD_PLAN D   
    on D.EFFDT = S.EFFDT 
   and D.INSTITUTION_CD = S.INSTITUTION_CD
   and D.ACAD_PLAN_CD = S.ACAD_PLAN_CD
   and D.SRC_SYS_ID = S.SRC_SYS_ID
  left outer join D2
    on S.INSTITUTION_CD = D2.INSTITUTION_CD 
   and S.ACAD_PLAN_CD = D2.ACAD_PLAN_CD
   and S.SRC_SYS_ID = D2.SRC_SYS_ID 
  left outer join N3
    on S.INSTITUTION_CD = N3.INSTITUTION 
   and S.ACAD_PLAN_CD = N3.ACAD_PLAN
   and S.SRC_SYS_ID = N3.SRC_SYS_ID 
) S 
    on (T.EFFDT = S.EFFDT
   and  T.INSTITUTION_CD = S.INSTITUTION_CD 
   and  T.ACAD_PLAN_CD = S.ACAD_PLAN_CD 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)
  when matched then update set 
       T.EFFDT_START = S.EFFDT_START,
       T.EFFDT_END = S.EFFDT_END,
       T.EFFDT_ORDER = S.EFFDT_ORDER,
       T.EFF_STAT_CD = S.EFF_STAT_CD,
       T.ACAD_PLAN_SD = S.ACAD_PLAN_SD,
       T.ACAD_PLAN_LD = S.ACAD_PLAN_LD,
       T.ACAD_PLAN_CD_DESC = S.ACAD_PLAN_CD_DESC,
       T.ACAD_CAR_SID = S.ACAD_CAR_SID,
       T.ACAD_PROG_SID = S.ACAD_PROG_SID,
       T.DEG_SID = S.DEG_SID,
       T.INSTITUTION_SID = S.INSTITUTION_SID,
       T.ACAD_PLAN_TYPE_CD = S.ACAD_PLAN_TYPE_CD,
       T.ACAD_PLAN_TYPE_SD = S.ACAD_PLAN_TYPE_SD,
       T.ACAD_PLAN_TYPE_LD = S.ACAD_PLAN_TYPE_LD,
       T.ACAD_PLAN_TYPE_CD_DESC = S.ACAD_PLAN_TYPE_CD_DESC,
       T.CIP_CD = S.CIP_CD,
       T.CIP_LD = S.CIP_LD,
       T.DIPLOMA_DESCR = S.DIPLOMA_DESCR,
       T.DIPLOMA_PRINT_FLG = S.DIPLOMA_PRINT_FLG,
       T.EDU_LVL_CTGRY = S.EDU_LVL_CTGRY,
       T.EVALUATE_PLAN_FLG = S.EVALUATE_PLAN_FLG,
       T.PLAN_REQTRM_DFLT = S.PLAN_REQTRM_DFLT,
       T.PLAN_REQTRM_DFLT_SD = S.PLAN_REQTRM_DFLT_SD,
       T.PLAN_REQTRM_DFLT_LD = S.PLAN_REQTRM_DFLT_LD,
       T.SAA_WHIF_DISP_ADVR_FLG = S.SAA_WHIF_DISP_ADVR_FLG,
       T.SAA_WHIF_DISP_PREM_FLG = S.SAA_WHIF_DISP_PREM_FLG,
       T.SAA_WHIF_DISP_STD_FLG = S.SAA_WHIF_DISP_STD_FLG,
       T.SSR_NSC_CRD_LVL = S.SSR_NSC_CRD_LVL,
       T.SSR_NSC_CRD_LVL_SD = S.SSR_NSC_CRD_LVL_SD,
       T.SSR_NSC_CRD_LVL_LD = S.SSR_NSC_CRD_LVL_LD,
       T.SSR_NSC_INCL_PLAN_FLG = S.SSR_NSC_INCL_PLAN_FLG,
       T.SSR_PROG_LEN_TYPE = S.SSR_PROG_LEN_TYPE,
       T.SSR_PROG_LEN_TYPE_SD = S.SSR_PROG_LEN_TYPE_SD,
       T.SSR_PROG_LEN_TYPE_LD = S.SSR_PROG_LEN_TYPE_LD,
       T.SSR_PROG_LENGTH = S.SSR_PROG_LENGTH,
       T.SEV_VALID_CIP_FLG = S.SEV_VALID_CIP_FLG,
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
       decode(T.ACAD_PLAN_SD,S.ACAD_PLAN_SD,0,1) = 1 or 
       decode(T.ACAD_PLAN_LD,S.ACAD_PLAN_LD,0,1) = 1 or 
       decode(T.ACAD_PLAN_CD_DESC,S.ACAD_PLAN_CD_DESC,0,1) = 1 or 
       decode(T.ACAD_CAR_SID,S.ACAD_CAR_SID,0,1) = 1 or 
       decode(T.ACAD_PROG_SID,S.ACAD_PROG_SID,0,1) = 1 or 
       decode(T.DEG_SID,S.DEG_SID,0,1) = 1 or 
       decode(T.INSTITUTION_SID,S.INSTITUTION_SID,0,1) = 1 or 
       decode(T.ACAD_PLAN_TYPE_CD,S.ACAD_PLAN_TYPE_CD,0,1) = 1 or 
       decode(T.ACAD_PLAN_TYPE_SD,S.ACAD_PLAN_TYPE_SD,0,1) = 1 or 
       decode(T.ACAD_PLAN_TYPE_LD,S.ACAD_PLAN_TYPE_LD,0,1) = 1 or 
       decode(T.ACAD_PLAN_TYPE_CD_DESC,S.ACAD_PLAN_TYPE_CD_DESC,0,1) = 1 or 
       decode(T.CIP_CD,S.CIP_CD,0,1) = 1 or 
       decode(T.CIP_LD,S.CIP_LD,0,1) = 1 or 
       decode(T.DIPLOMA_DESCR,S.DIPLOMA_DESCR,0,1) = 1 or 
       decode(T.DIPLOMA_PRINT_FLG,S.DIPLOMA_PRINT_FLG,0,1) = 1 or 
       decode(T.EDU_LVL_CTGRY,S.EDU_LVL_CTGRY,0,1) = 1 or 
       decode(T.EVALUATE_PLAN_FLG,S.EVALUATE_PLAN_FLG,0,1) = 1 or 
       decode(T.PLAN_REQTRM_DFLT,S.PLAN_REQTRM_DFLT,0,1) = 1 or 
       decode(T.PLAN_REQTRM_DFLT_SD,S.PLAN_REQTRM_DFLT_SD,0,1) = 1 or 
       decode(T.PLAN_REQTRM_DFLT_LD,S.PLAN_REQTRM_DFLT_LD,0,1) = 1 or 
       decode(T.SAA_WHIF_DISP_ADVR_FLG,S.SAA_WHIF_DISP_ADVR_FLG,0,1) = 1 or 
       decode(T.SAA_WHIF_DISP_PREM_FLG,S.SAA_WHIF_DISP_PREM_FLG,0,1) = 1 or 
       decode(T.SAA_WHIF_DISP_STD_FLG,S.SAA_WHIF_DISP_STD_FLG,0,1) = 1 or 
       decode(T.SSR_NSC_CRD_LVL,S.SSR_NSC_CRD_LVL,0,1) = 1 or 
       decode(T.SSR_NSC_CRD_LVL_SD,S.SSR_NSC_CRD_LVL_SD,0,1) = 1 or 
       decode(T.SSR_NSC_CRD_LVL_LD,S.SSR_NSC_CRD_LVL_LD,0,1) = 1 or 
       decode(T.SSR_NSC_INCL_PLAN_FLG,S.SSR_NSC_INCL_PLAN_FLG,0,1) = 1 or 
       decode(T.SSR_PROG_LEN_TYPE,S.SSR_PROG_LEN_TYPE,0,1) = 1 or 
       decode(T.SSR_PROG_LEN_TYPE_SD,S.SSR_PROG_LEN_TYPE_SD,0,1) = 1 or 
       decode(T.SSR_PROG_LEN_TYPE_LD,S.SSR_PROG_LEN_TYPE_LD,0,1) = 1 or 
       decode(T.SSR_PROG_LENGTH,S.SSR_PROG_LENGTH,0,1) = 1 or 
       decode(T.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG,0,1) = 1 or 
       decode(T.TRNSCR_DESCR,S.TRNSCR_DESCR,0,1) = 1 or 
       decode(T.TRNSCR_PRINT_FLG,S.TRNSCR_PRINT_FLG,0,1) = 1 or 
       decode(T.UM_STEM_FLG,S.UM_STEM_FLG,0,1) = 1 or
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
  when not matched then
insert (
       T.ACAD_PLAN_SID,
       T.EFFDT,
       T.INSTITUTION_CD,
       T.ACAD_PLAN_CD,
       T.SRC_SYS_ID,
       T.EFFDT_START,
       T.EFFDT_END,
       T.EFFDT_ORDER,
       T.EFF_STAT_CD,
       T.ACAD_PLAN_SD,
       T.ACAD_PLAN_LD,
       T.ACAD_PLAN_CD_DESC,
       T.ACAD_CAR_SID,
       T.ACAD_PROG_SID,
       T.DEG_SID,
       T.INSTITUTION_SID,
       T.ACAD_PLAN_TYPE_CD,
       T.ACAD_PLAN_TYPE_SD,
       T.ACAD_PLAN_TYPE_LD,
       T.ACAD_PLAN_TYPE_CD_DESC,
       T.CIP_CD,
       T.CIP_LD,
       T.DIPLOMA_DESCR,
       T.DIPLOMA_PRINT_FLG,
       T.EDU_LVL_CTGRY,
       T.EVALUATE_PLAN_FLG,
       T.PLAN_REQTRM_DFLT,
       T.PLAN_REQTRM_DFLT_SD,
       T.PLAN_REQTRM_DFLT_LD,
       T.SAA_WHIF_DISP_ADVR_FLG,
       T.SAA_WHIF_DISP_PREM_FLG,
       T.SAA_WHIF_DISP_STD_FLG,
       T.SSR_NSC_CRD_LVL,
       T.SSR_NSC_CRD_LVL_SD,
       T.SSR_NSC_CRD_LVL_LD,
       T.SSR_NSC_INCL_PLAN_FLG,
       T.SSR_PROG_LEN_TYPE,
       T.SSR_PROG_LEN_TYPE_SD,
       T.SSR_PROG_LEN_TYPE_LD,
       T.SSR_PROG_LENGTH,
       T.SEV_VALID_CIP_FLG,
       T.TRNSCR_DESCR,
       T.TRNSCR_PRINT_FLG,
       T.UM_STEM_FLG,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values (
       S.ACAD_PLAN_SID,
       S.EFFDT,
       S.INSTITUTION_CD,
       S.ACAD_PLAN_CD,
       S.SRC_SYS_ID,
       S.EFFDT_START,
       S.EFFDT_END,
       S.EFFDT_ORDER,
       S.EFF_STAT_CD,
       S.ACAD_PLAN_SD,
       S.ACAD_PLAN_LD,
       S.ACAD_PLAN_CD_DESC,
       S.ACAD_CAR_SID,
       S.ACAD_PROG_SID,
       S.DEG_SID,
       S.INSTITUTION_SID,
       S.ACAD_PLAN_TYPE_CD,
       S.ACAD_PLAN_TYPE_SD,
       S.ACAD_PLAN_TYPE_LD,
       S.ACAD_PLAN_TYPE_CD_DESC,
       S.CIP_CD,
       S.CIP_LD,
       S.DIPLOMA_DESCR,
       S.DIPLOMA_PRINT_FLG,
       S.EDU_LVL_CTGRY,
       S.EVALUATE_PLAN_FLG,
       S.PLAN_REQTRM_DFLT,
       S.PLAN_REQTRM_DFLT_SD,
       S.PLAN_REQTRM_DFLT_LD,
       S.SAA_WHIF_DISP_ADVR_FLG,
       S.SAA_WHIF_DISP_PREM_FLG,
       S.SAA_WHIF_DISP_STD_FLG,
       S.SSR_NSC_CRD_LVL,
       S.SSR_NSC_CRD_LVL_SD,
       S.SSR_NSC_CRD_LVL_LD,
       S.SSR_NSC_INCL_PLAN_FLG,
       S.SSR_PROG_LEN_TYPE,
       S.SSR_PROG_LEN_TYPE_SD,
       S.SSR_PROG_LEN_TYPE_LD,
       S.SSR_PROG_LENGTH,
       S.SEV_VALID_CIP_FLG,
       S.TRNSCR_DESCR,
       S.TRNSCR_PRINT_FLG,
       S.UM_STEM_FLG,
       S.DATA_ORIGIN,
       S.CREATED_EW_DTTM,
       S.LASTUPD_EW_DTTM)
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_ACAD_PLAN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_ACAD_PLAN';
update CSMRT_OWNER.UM_D_ACAD_PLAN T   
   set EFFDT_START = '31-DEC-9999',
       EFFDT_ORDER = 9,
       DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ACAD_PLAN_SID <> 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_ACAD_PLAN_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.ACAD_PLAN_CD = S.ACAD_PLAN
                      and T.EFFDT = S.EFFDT
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D');

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_ACAD_PLAN rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_ACAD_PLAN',
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

END UM_D_ACAD_PLAN_P;
/
