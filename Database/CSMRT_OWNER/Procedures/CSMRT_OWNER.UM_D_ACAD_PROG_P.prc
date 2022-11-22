DROP PROCEDURE CSMRT_OWNER.UM_D_ACAD_PROG_P
/

--
-- UM_D_ACAD_PROG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_ACAD_PROG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- UM_D_ACAD_PROG / UM_D_ACAD_PROG_VW
--Loads target table       -- UM_D_ACAD_PROG
--UM_D_ACAD_PROG           -- Dependent on PS_D_INSTITUTION, PS_D_ACAD_CAR, PS_D_ACAD_GRP, PS_D_ACAD_ORG, PS_D_CAMPUS
-- V01 4/22/2018           -- srikanth ,pabbu converted to proc from sql
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_ACAD_PROG';
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

strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_ACAD_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_ACAD_PROG';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_ACAD_PROG T 
using (
  with CIP as (
select CIP_CODE, EFFDT, SRC_SYS_ID,
       decode(max(EFFDT) over (partition by CIP_CODE, SRC_SYS_ID
                                   order by EFFDT 
                               rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),EFFDT) EFFDT_START,   
       nvl(min(EFFDT-1) over (partition by CIP_CODE, SRC_SYS_ID
                                  order by EFFDT
                              rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,   
       DESCR, SEV_VALID_CIP_CD, UM_STEM,
       row_number() over (partition by CIP_CODE, SRC_SYS_ID
                              order by EFFDT desc) CIP_ORDER
  from CSSTG_OWNER.PS_CIP_CODE_TBL
 where DATA_ORIGIN <> 'D'),
       XL as (
select FIELDNAME, FIELDVALUE, SRC_SYS_ID, XLATLONGNAME, XLATSHORTNAME
  from UM_D_XLATITEM
 where SRC_SYS_ID = 'CS90'),
       D2 as (
select distinct 
       ACAD_PROG_SID, INSTITUTION_CD, ACAD_PROG_CD, SRC_SYS_ID
  from CSMRT_OWNER.UM_D_ACAD_PROG),   
       N1 as (
select distinct 
       INSTITUTION, ACAD_PROG, SRC_SYS_ID
  from CSSTG_OWNER.PS_ACAD_PROG_TBL
 where DATA_ORIGIN <> 'D'
 minus
select INSTITUTION_CD, ACAD_PROG_CD, SRC_SYS_ID
  from CSMRT_OWNER.UM_D_ACAD_PROG),   
       N2 as (
select max(ACAD_PROG_SID) MAX_SID
  from CSMRT_OWNER.UM_D_ACAD_PROG   
 where ACAD_PROG_SID <> 2147483646),
       N3 as (
select N1.INSTITUTION, N1.ACAD_PROG, N1.SRC_SYS_ID, 
       nvl(N2.MAX_SID,0) + row_number() over (partition by 1 order by N1.INSTITUTION, N1.ACAD_PROG, N1.SRC_SYS_ID nulls first) NEW_SID
  from N1, N2),
       S as (
select G.EFFDT,                         -- Moved to PK
       G.INSTITUTION INSTITUTION_CD, 
       G.ACAD_PROG ACAD_PROG_CD, 
       G.SRC_SYS_ID, 
       decode(max(G.EFFDT) over (partition by G.INSTITUTION, G.ACAD_PROG, G.SRC_SYS_ID
                                        order by G.EFFDT 
                                    rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),G.EFFDT) EFFDT_START,      -- Added  
       nvl(min(G.EFFDT-1) over (partition by G.INSTITUTION, G.ACAD_PROG, G.SRC_SYS_ID
                                    order by G.EFFDT
                                rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,      -- Added  
       row_number() over (partition by G.INSTITUTION, G.ACAD_PROG, G.SRC_SYS_ID
                              order by G.EFFDT desc) EFFDT_ORDER,                                                         -- Added 
       G.EFF_STATUS EFF_STAT_CD, 
       G.DESCRSHORT ACAD_PROG_SD,
       G.DESCR ACAD_PROG_LD, 
       G.ACAD_PROG||' ('||G.DESCR||')' ACAD_PROG_CD_DESC,       -- Added 
       nvl(C.ACAD_CAR_SID,2147483646) ACAD_CAR_SID, 
       nvl(GRP.ACAD_GRP_SID,2147483646) ACAD_GRP_SID, 
       nvl(ORG.ACAD_ORG_SID,2147483646) ACAD_ORG_SID, 
       nvl(CMP.CAMPUS_SID,2147483646) CAMPUS_SID, 
       nvl(I.INSTITUTION_SID,2147483646) INSTITUTION_SID, 
       G.ACAD_STDNG_RULE, 
       G.CALC_AS_BATCH_ONLY CALC_AS_BATCH_ONLY_FLG, 
       G.CAR_PTR_EXC_FG CAR_PTR_EXC_FLG, 
       G.CAR_PTR_EXC_RULE, 
       G.CIP_CODE CIP_CD, 
       nvl(CIP.DESCR,'-') CIP_LD,
       G.FA_ELIGIBILITY FA_ELIG_FLG, 
       G.FA_PRIMACY_NBR, 
       G.GRADE_TRANSFER, 
       G.GRADING_SCHEME, 
       G.GRADING_BASIS, 
       G.INCOMPLETE_GRADE, 
       G.LAPSE_GRADE LAPSE_GRADE_FLG, 
       G.LAPSE_TO_GRADE, 
       G.LAPSE_DAYS, 
       G.LEVEL_LOAD_RULE, 
       G.OEE_IND, 
       G.REPEAT_ENRL_CTL REPEAT_ENRL_CTL_FLG, 
       G.REPEAT_GRD_SUSP REPEAT_ENRL_SUSP_FLG, 
       G.REPEAT_GRD_CK, 
       nvl(X1.XLATSHORTNAME,'-') REPEAT_GRD_CK_SD,
       nvl(X1.XLATLONGNAME,'-') REPEAT_GRD_CK_LD,
       G.REPEAT_GRD_SUSP REPEAT_GRD_SUSP_FLG, 
       G.REPEAT_CRSE_ERROR, 
       nvl(X2.XLATSHORTNAME,'-') REPEAT_CRSE_ERROR_SD,
       nvl(X2.XLATLONGNAME,'-') REPEAT_CRSE_ERROR_LD,
       G.REPEAT_RULE, 
       G.RESIDENCY_REQ RES_REQ_FLG, 
       nvl(CIP.SEV_VALID_CIP_CD,'-') SEV_VALID_CIP_FLG,
       nvl(CIP.UM_STEM,'-') UM_STEM_FLG
  from CSSTG_OWNER.PS_ACAD_PROG_TBL G
  left outer join CIP
    on G.CIP_CODE = CIP.CIP_CODE
   and G.SRC_SYS_ID = CIP.SRC_SYS_ID
   and G.EFFDT between CIP.EFFDT_START and CIP.EFFDT_END
  join PS_D_INSTITUTION I  
    on G.INSTITUTION = I.INSTITUTION_CD
   and G.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_CAR C  
    on G.INSTITUTION = C.INSTITUTION_CD
   and G.ACAD_CAREER = C.ACAD_CAR_CD
   and G.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_GRP GRP  
    on G.INSTITUTION = GRP.INSTITUTION_CD
   and G.ACAD_GROUP = GRP.ACAD_GRP_CD
   and G.SRC_SYS_ID = GRP.SRC_SYS_ID
   and GRP.EFFDT_ORDER = 1 
   and GRP.DATA_ORIGIN <> 'D'
  left outer join PS_D_ACAD_ORG ORG  
    on G.ACAD_ORG = ORG.ACAD_ORG_CD
   and G.SRC_SYS_ID = ORG.SRC_SYS_ID
   and ORG.EFFDT_ORDER = 1 
   and ORG.DATA_ORIGIN <> 'D'
  left outer join PS_D_CAMPUS CMP  
    on G.INSTITUTION = CMP.INSTITUTION_CD
   and G.CAMPUS = CMP.CAMPUS_CD
   and G.SRC_SYS_ID = CMP.SRC_SYS_ID
   and CMP.DATA_ORIGIN <> 'D'
  left outer join XL X1
    on X1.FIELDNAME = 'REPEAT_GRD_CK'
   and X1.FIELDVALUE = G.REPEAT_GRD_CK 
  left outer join XL X2
    on X2.FIELDNAME = 'REPEAT_CRSE_ERROR'
   and X2.FIELDVALUE = G.REPEAT_CRSE_ERROR 
 where G.DATA_ORIGIN <> 'D')
select nvl(nvl(D.ACAD_PROG_SID, D2.ACAD_PROG_SID), N3.NEW_SID) ACAD_PROG_SID,
       nvl(D.EFFDT, S.EFFDT) EFFDT,                                                
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       nvl(D.ACAD_PROG_CD, S.ACAD_PROG_CD) ACAD_PROG_CD, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.EFFDT_START,S.EFFDT_START,D.EFFDT_START,S.EFFDT_START) EFFDT_START,
       decode(D.EFFDT_END,S.EFFDT_END,D.EFFDT_END,S.EFFDT_END) EFFDT_END,
       decode(D.EFFDT_ORDER,S.EFFDT_ORDER,D.EFFDT_ORDER,S.EFFDT_ORDER) EFFDT_ORDER,
       decode(D.EFF_STAT_CD,S.EFF_STAT_CD,D.EFF_STAT_CD,S.EFF_STAT_CD) EFF_STAT_CD,
       decode(D.ACAD_PROG_SD,S.ACAD_PROG_SD,D.ACAD_PROG_SD,S.ACAD_PROG_SD) ACAD_PROG_SD,
       decode(D.ACAD_PROG_LD,S.ACAD_PROG_LD,D.ACAD_PROG_LD,S.ACAD_PROG_LD) ACAD_PROG_LD,
       decode(D.ACAD_PROG_CD_DESC,S.ACAD_PROG_CD_DESC,D.ACAD_PROG_CD_DESC,S.ACAD_PROG_CD_DESC) ACAD_PROG_CD_DESC,
       decode(D.ACAD_CAR_SID,S.ACAD_CAR_SID,D.ACAD_CAR_SID,S.ACAD_CAR_SID) ACAD_CAR_SID,
       decode(D.ACAD_GRP_SID,S.ACAD_GRP_SID,D.ACAD_GRP_SID,S.ACAD_GRP_SID) ACAD_GRP_SID,
       decode(D.ACAD_ORG_SID,S.ACAD_ORG_SID,D.ACAD_ORG_SID,S.ACAD_ORG_SID) ACAD_ORG_SID,
       decode(D.CAMPUS_SID,S.CAMPUS_SID,D.CAMPUS_SID,S.CAMPUS_SID) CAMPUS_SID,
       decode(D.INSTITUTION_SID,S.INSTITUTION_SID,D.INSTITUTION_SID,S.INSTITUTION_SID) INSTITUTION_SID,
       decode(D.ACAD_STDNG_RULE,S.ACAD_STDNG_RULE,D.ACAD_STDNG_RULE,S.ACAD_STDNG_RULE) ACAD_STDNG_RULE,
       decode(D.CALC_AS_BATCH_ONLY_FLG,S.CALC_AS_BATCH_ONLY_FLG,D.CALC_AS_BATCH_ONLY_FLG,S.CALC_AS_BATCH_ONLY_FLG) CALC_AS_BATCH_ONLY_FLG,
       decode(D.CAR_PTR_EXC_FLG,S.CAR_PTR_EXC_FLG,D.CAR_PTR_EXC_FLG,S.CAR_PTR_EXC_FLG) CAR_PTR_EXC_FLG,
       decode(D.CAR_PTR_EXC_RULE,S.CAR_PTR_EXC_RULE,D.CAR_PTR_EXC_RULE,S.CAR_PTR_EXC_RULE) CAR_PTR_EXC_RULE,
       decode(D.CIP_CD,S.CIP_CD,D.CIP_CD,S.CIP_CD) CIP_CD,
       decode(D.CIP_LD,S.CIP_LD,D.CIP_LD,S.CIP_LD) CIP_LD,
       decode(D.FA_ELIG_FLG,S.FA_ELIG_FLG,D.FA_ELIG_FLG,S.FA_ELIG_FLG) FA_ELIG_FLG,
       decode(D.FA_PRIMACY_NBR,S.FA_PRIMACY_NBR,D.FA_PRIMACY_NBR,S.FA_PRIMACY_NBR) FA_PRIMACY_NBR,
       decode(D.GRADE_TRANSFER,S.GRADE_TRANSFER,D.GRADE_TRANSFER,S.GRADE_TRANSFER) GRADE_TRANSFER,
       decode(D.GRADING_SCHEME,S.GRADING_SCHEME,D.GRADING_SCHEME,S.GRADING_SCHEME) GRADING_SCHEME,
       decode(D.GRADING_BASIS,S.GRADING_BASIS,D.GRADING_BASIS,S.GRADING_BASIS) GRADING_BASIS,
       decode(D.INCOMPLETE_GRADE,S.INCOMPLETE_GRADE,D.INCOMPLETE_GRADE,S.INCOMPLETE_GRADE) INCOMPLETE_GRADE,
       decode(D.LAPSE_GRADE_FLG,S.LAPSE_GRADE_FLG,D.LAPSE_GRADE_FLG,S.LAPSE_GRADE_FLG) LAPSE_GRADE_FLG,
       decode(D.LAPSE_TO_GRADE,S.LAPSE_TO_GRADE,D.LAPSE_TO_GRADE,S.LAPSE_TO_GRADE) LAPSE_TO_GRADE,
       decode(D.LAPSE_DAYS,S.LAPSE_DAYS,D.LAPSE_DAYS,S.LAPSE_DAYS) LAPSE_DAYS,
       decode(D.LEVEL_LOAD_RULE,S.LEVEL_LOAD_RULE,D.LEVEL_LOAD_RULE,S.LEVEL_LOAD_RULE) LEVEL_LOAD_RULE,
       decode(D.OEE_IND,S.OEE_IND,D.OEE_IND,S.OEE_IND) OEE_IND,
       decode(D.REPEAT_ENRL_CTL_FLG,S.REPEAT_ENRL_CTL_FLG,D.REPEAT_ENRL_CTL_FLG,S.REPEAT_ENRL_CTL_FLG) REPEAT_ENRL_CTL_FLG,
       decode(D.REPEAT_ENRL_SUSP_FLG,S.REPEAT_ENRL_SUSP_FLG,D.REPEAT_ENRL_SUSP_FLG,S.REPEAT_ENRL_SUSP_FLG) REPEAT_ENRL_SUSP_FLG,
       decode(D.REPEAT_GRD_CK,S.REPEAT_GRD_CK,D.REPEAT_GRD_CK,S.REPEAT_GRD_CK) REPEAT_GRD_CK,
       decode(D.REPEAT_GRD_CK_SD,S.REPEAT_GRD_CK_SD,D.REPEAT_GRD_CK_SD,S.REPEAT_GRD_CK_SD) REPEAT_GRD_CK_SD,
       decode(D.REPEAT_GRD_CK_LD,S.REPEAT_GRD_CK_LD,D.REPEAT_GRD_CK_LD,S.REPEAT_GRD_CK_LD) REPEAT_GRD_CK_LD,
       decode(D.REPEAT_GRD_SUSP_FLG,S.REPEAT_GRD_SUSP_FLG,D.REPEAT_GRD_SUSP_FLG,S.REPEAT_GRD_SUSP_FLG) REPEAT_GRD_SUSP_FLG,
       decode(D.REPEAT_CRSE_ERROR,S.REPEAT_CRSE_ERROR,D.REPEAT_CRSE_ERROR,S.REPEAT_CRSE_ERROR) REPEAT_CRSE_ERROR,
       decode(D.REPEAT_CRSE_ERROR_SD,S.REPEAT_CRSE_ERROR_SD,D.REPEAT_CRSE_ERROR_SD,S.REPEAT_CRSE_ERROR_SD) REPEAT_CRSE_ERROR_SD,
       decode(D.REPEAT_CRSE_ERROR_LD,S.REPEAT_CRSE_ERROR_LD,D.REPEAT_CRSE_ERROR_LD,S.REPEAT_CRSE_ERROR_LD) REPEAT_CRSE_ERROR_LD,
       decode(D.REPEAT_RULE,S.REPEAT_RULE,D.REPEAT_RULE,S.REPEAT_RULE) REPEAT_RULE,
       decode(D.RES_REQ_FLG,S.RES_REQ_FLG,D.RES_REQ_FLG,S.RES_REQ_FLG) RES_REQ_FLG,
       decode(D.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG,D.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG) SEV_VALID_CIP_FLG,
       decode(D.UM_STEM_FLG,S.UM_STEM_FLG,D.UM_STEM_FLG,S.UM_STEM_FLG) UM_STEM_FLG,
       decode(D.DATA_ORIGIN,'S',D.DATA_ORIGIN,'S') DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S
  left outer join CSMRT_OWNER.UM_D_ACAD_PROG D   
    on D.EFFDT = S.EFFDT 
   and D.INSTITUTION_CD = S.INSTITUTION_CD 
   and D.ACAD_PROG_CD = S.ACAD_PROG_CD 
   and D.SRC_SYS_ID = S.SRC_SYS_ID 
   and D.ACAD_PROG_SID <> 2147483646
  left outer join D2
    on S.INSTITUTION_CD = D2.INSTITUTION_CD 
   and S.ACAD_PROG_CD = D2.ACAD_PROG_CD
   and S.SRC_SYS_ID = D2.SRC_SYS_ID 
  left outer join N3
    on S.INSTITUTION_CD = N3.INSTITUTION 
   and S.ACAD_PROG_CD = N3.ACAD_PROG
   and S.SRC_SYS_ID = N3.SRC_SYS_ID) S 
    on (T.EFFDT = S.EFFDT
   and  T.INSTITUTION_CD = S.INSTITUTION_CD 
   and  T.ACAD_PROG_CD = S.ACAD_PROG_CD 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)
  when matched then update set 
       T.EFFDT_START = S.EFFDT_START,
       T.EFFDT_END = S.EFFDT_END,
       T.EFFDT_ORDER = S.EFFDT_ORDER,
       T.EFF_STAT_CD = S.EFF_STAT_CD,
       T.ACAD_PROG_SD = S.ACAD_PROG_SD,
       T.ACAD_PROG_LD = S.ACAD_PROG_LD,
       T.ACAD_PROG_CD_DESC = S.ACAD_PROG_CD_DESC,
       T.ACAD_CAR_SID = S.ACAD_CAR_SID,
       T.ACAD_GRP_SID = S.ACAD_GRP_SID,
       T.ACAD_ORG_SID = S.ACAD_ORG_SID,
       T.CAMPUS_SID = S.CAMPUS_SID,
       T.INSTITUTION_SID = S.INSTITUTION_SID,
       T.ACAD_STDNG_RULE = S.ACAD_STDNG_RULE,
       T.CALC_AS_BATCH_ONLY_FLG = S.CALC_AS_BATCH_ONLY_FLG,
       T.CAR_PTR_EXC_FLG = S.CAR_PTR_EXC_FLG,
       T.CAR_PTR_EXC_RULE = S.CAR_PTR_EXC_RULE,
       T.CIP_CD = S.CIP_CD,
       T.CIP_LD = S.CIP_LD,
       T.FA_ELIG_FLG = S.FA_ELIG_FLG,
       T.FA_PRIMACY_NBR = S.FA_PRIMACY_NBR,
       T.GRADE_TRANSFER = S.GRADE_TRANSFER,
       T.GRADING_SCHEME = S.GRADING_SCHEME,
       T.GRADING_BASIS = S.GRADING_BASIS,
       T.INCOMPLETE_GRADE = S.INCOMPLETE_GRADE,
       T.LAPSE_GRADE_FLG = S.LAPSE_GRADE_FLG,
       T.LAPSE_TO_GRADE = S.LAPSE_TO_GRADE,
       T.LAPSE_DAYS = S.LAPSE_DAYS,
       T.LEVEL_LOAD_RULE = S.LEVEL_LOAD_RULE,
       T.OEE_IND = S.OEE_IND,
       T.REPEAT_ENRL_CTL_FLG = S.REPEAT_ENRL_CTL_FLG,
       T.REPEAT_ENRL_SUSP_FLG = S.REPEAT_ENRL_SUSP_FLG,
       T.REPEAT_GRD_CK = S.REPEAT_GRD_CK,
       T.REPEAT_GRD_CK_SD = S.REPEAT_GRD_CK_SD,
       T.REPEAT_GRD_CK_LD = S.REPEAT_GRD_CK_LD,
       T.REPEAT_GRD_SUSP_FLG = S.REPEAT_GRD_SUSP_FLG,
       T.REPEAT_CRSE_ERROR = S.REPEAT_CRSE_ERROR,
       T.REPEAT_CRSE_ERROR_SD = S.REPEAT_CRSE_ERROR_SD,
       T.REPEAT_CRSE_ERROR_LD = S.REPEAT_CRSE_ERROR_LD,
       T.REPEAT_RULE = S.REPEAT_RULE,
       T.RES_REQ_FLG = S.RES_REQ_FLG,
       T.SEV_VALID_CIP_FLG = S.SEV_VALID_CIP_FLG,
       T.UM_STEM_FLG = S.UM_STEM_FLG,
       T.DATA_ORIGIN = S.DATA_ORIGIN,
       T.LASTUPD_EW_DTTM = S.LASTUPD_EW_DTTM
 where 
       decode(T.EFFDT_START,S.EFFDT_START,0,1) = 1 or 
       decode(T.EFFDT_END,S.EFFDT_END,0,1) = 1 or 
       decode(T.EFFDT_ORDER,S.EFFDT_ORDER,0,1) = 1 or 
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or 
       decode(T.ACAD_PROG_SD,S.ACAD_PROG_SD,0,1) = 1 or 
       decode(T.ACAD_PROG_LD,S.ACAD_PROG_LD,0,1) = 1 or 
       decode(T.ACAD_PROG_CD_DESC,S.ACAD_PROG_CD_DESC,0,1) = 1 or 
       decode(T.ACAD_CAR_SID,S.ACAD_CAR_SID,0,1) = 1 or 
       decode(T.ACAD_GRP_SID,S.ACAD_GRP_SID,0,1) = 1 or 
       decode(T.ACAD_ORG_SID,S.ACAD_ORG_SID,0,1) = 1 or 
       decode(T.CAMPUS_SID,S.CAMPUS_SID,0,1) = 1 or 
       decode(T.INSTITUTION_SID,S.INSTITUTION_SID,0,1) = 1 or 
       decode(T.ACAD_STDNG_RULE,S.ACAD_STDNG_RULE,0,1) = 1 or 
       decode(T.CALC_AS_BATCH_ONLY_FLG,S.CALC_AS_BATCH_ONLY_FLG,0,1) = 1 or 
       decode(T.CAR_PTR_EXC_FLG,S.CAR_PTR_EXC_FLG,0,1) = 1 or 
       decode(T.CAR_PTR_EXC_RULE,S.CAR_PTR_EXC_RULE,0,1) = 1 or 
       decode(T.CIP_CD,S.CIP_CD,0,1) = 1 or 
       decode(T.CIP_LD,S.CIP_LD,0,1) = 1 or 
       decode(T.FA_ELIG_FLG,S.FA_ELIG_FLG,0,1) = 1 or 
       decode(T.FA_PRIMACY_NBR,S.FA_PRIMACY_NBR,0,1) = 1 or 
       decode(T.GRADE_TRANSFER,S.GRADE_TRANSFER,0,1) = 1 or 
       decode(T.GRADING_SCHEME,S.GRADING_SCHEME,0,1) = 1 or 
       decode(T.GRADING_BASIS,S.GRADING_BASIS,0,1) = 1 or 
       decode(T.INCOMPLETE_GRADE,S.INCOMPLETE_GRADE,0,1) = 1 or 
       decode(T.LAPSE_GRADE_FLG,S.LAPSE_GRADE_FLG,0,1) = 1 or 
       decode(T.LAPSE_TO_GRADE,S.LAPSE_TO_GRADE,0,1) = 1 or 
       decode(T.LAPSE_DAYS,S.LAPSE_DAYS,0,1) = 1 or 
       decode(T.LEVEL_LOAD_RULE,S.LEVEL_LOAD_RULE,0,1) = 1 or 
       decode(T.OEE_IND,S.OEE_IND,0,1) = 1 or 
       decode(T.REPEAT_ENRL_CTL_FLG,S.REPEAT_ENRL_CTL_FLG,0,1) = 1 or 
       decode(T.REPEAT_ENRL_SUSP_FLG,S.REPEAT_ENRL_SUSP_FLG,0,1) = 1 or 
       decode(T.REPEAT_GRD_CK,S.REPEAT_GRD_CK,0,1) = 1 or 
       decode(T.REPEAT_GRD_CK_SD,S.REPEAT_GRD_CK_SD,0,1) = 1 or 
       decode(T.REPEAT_GRD_CK_LD,S.REPEAT_GRD_CK_LD,0,1) = 1 or 
       decode(T.REPEAT_GRD_SUSP_FLG,S.REPEAT_GRD_SUSP_FLG,0,1) = 1 or 
       decode(T.REPEAT_CRSE_ERROR,S.REPEAT_CRSE_ERROR,0,1) = 1 or 
       decode(T.REPEAT_CRSE_ERROR_SD,S.REPEAT_CRSE_ERROR_SD,0,1) = 1 or 
       decode(T.REPEAT_CRSE_ERROR_LD,S.REPEAT_CRSE_ERROR_LD,0,1) = 1 or 
       decode(T.REPEAT_RULE,S.REPEAT_RULE,0,1) = 1 or 
       decode(T.RES_REQ_FLG,S.RES_REQ_FLG,0,1) = 1 or 
       decode(T.SEV_VALID_CIP_FLG,S.SEV_VALID_CIP_FLG,0,1) = 1 or 
       decode(T.UM_STEM_FLG,S.UM_STEM_FLG,0,1) = 1 or
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
  when not matched then
insert (
       T.ACAD_PROG_SID,
       T.EFFDT,
       T.INSTITUTION_CD,
       T.ACAD_PROG_CD,
       T.SRC_SYS_ID,
       T.EFFDT_START,
       T.EFFDT_END,
       T.EFFDT_ORDER,
       T.EFF_STAT_CD,
       T.ACAD_PROG_SD,
       T.ACAD_PROG_LD,
       T.ACAD_PROG_CD_DESC,
       T.ACAD_CAR_SID,
       T.ACAD_GRP_SID,
       T.ACAD_ORG_SID,
       T.CAMPUS_SID,
       T.INSTITUTION_SID,
       T.ACAD_STDNG_RULE,
       T.CALC_AS_BATCH_ONLY_FLG,
       T.CAR_PTR_EXC_FLG,
       T.CAR_PTR_EXC_RULE,
       T.CIP_CD,
       T.CIP_LD,
       T.FA_ELIG_FLG,
       T.FA_PRIMACY_NBR,
       T.GRADE_TRANSFER,
       T.GRADING_SCHEME,
       T.GRADING_BASIS,
       T.INCOMPLETE_GRADE,
       T.LAPSE_GRADE_FLG,
       T.LAPSE_TO_GRADE,
       T.LAPSE_DAYS,
       T.LEVEL_LOAD_RULE,
       T.OEE_IND,
       T.REPEAT_ENRL_CTL_FLG,
       T.REPEAT_ENRL_SUSP_FLG,
       T.REPEAT_GRD_CK,
       T.REPEAT_GRD_CK_SD,
       T.REPEAT_GRD_CK_LD,
       T.REPEAT_GRD_SUSP_FLG,
       T.REPEAT_CRSE_ERROR,
       T.REPEAT_CRSE_ERROR_SD,
       T.REPEAT_CRSE_ERROR_LD,
       T.REPEAT_RULE,
       T.RES_REQ_FLG,
       T.SEV_VALID_CIP_FLG,
       T.UM_STEM_FLG,
       T.DATA_ORIGIN,
       T.CREATED_EW_DTTM,
       T.LASTUPD_EW_DTTM)
values (
       S.ACAD_PROG_SID,
       S.EFFDT,
       S.INSTITUTION_CD,
       S.ACAD_PROG_CD,
       S.SRC_SYS_ID,
       S.EFFDT_START,
       S.EFFDT_END,
       S.EFFDT_ORDER,
       S.EFF_STAT_CD,
       S.ACAD_PROG_SD,
       S.ACAD_PROG_LD,
       S.ACAD_PROG_CD_DESC,
       S.ACAD_CAR_SID,
       S.ACAD_GRP_SID,
       S.ACAD_ORG_SID,
       S.CAMPUS_SID,
       S.INSTITUTION_SID,
       S.ACAD_STDNG_RULE,
       S.CALC_AS_BATCH_ONLY_FLG,
       S.CAR_PTR_EXC_FLG,
       S.CAR_PTR_EXC_RULE,
       S.CIP_CD,
       S.CIP_LD,
       S.FA_ELIG_FLG,
       S.FA_PRIMACY_NBR,
       S.GRADE_TRANSFER,
       S.GRADING_SCHEME,
       S.GRADING_BASIS,
       S.INCOMPLETE_GRADE,
       S.LAPSE_GRADE_FLG,
       S.LAPSE_TO_GRADE,
       S.LAPSE_DAYS,
       S.LEVEL_LOAD_RULE,
       S.OEE_IND,
       S.REPEAT_ENRL_CTL_FLG,
       S.REPEAT_ENRL_SUSP_FLG,
       S.REPEAT_GRD_CK,
       S.REPEAT_GRD_CK_SD,
       S.REPEAT_GRD_CK_LD,
       S.REPEAT_GRD_SUSP_FLG,
       S.REPEAT_CRSE_ERROR,
       S.REPEAT_CRSE_ERROR_SD,
       S.REPEAT_CRSE_ERROR_LD,
       S.REPEAT_RULE,
       S.RES_REQ_FLG,
       S.SEV_VALID_CIP_FLG,
       S.UM_STEM_FLG,
       S.DATA_ORIGIN,
       S.CREATED_EW_DTTM,
       S.LASTUPD_EW_DTTM)
;

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_ACAD_PROG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_ACAD_PROG';
update CSMRT_OWNER.UM_D_ACAD_PROG T   
   set EFFDT_START = '31-DEC-9999',
       EFFDT_ORDER = 9,
       DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ACAD_PROG_SID <> 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_ACAD_PROG_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.ACAD_PROG_CD = S.ACAD_PROG
                      and T.EFFDT = S.EFFDT
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_ACAD_PROG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_ACAD_PROG',
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

END UM_D_ACAD_PROG_P;
/
