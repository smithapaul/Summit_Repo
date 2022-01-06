CREATE OR REPLACE VIEW UM_M_AF_STDNT_VW
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
              ACAD_CAR_CD,
              TERM_CD,
              PERSON_ID,
              SRC_SYS_ID,
              ACAD_YR     ACAD_YR_SID,
              AID_YEAR,
              TERM_LD,
              ACAD_ORG_CD,                                           -- Sept 2018
              ACAD_ORG_LD,                                           -- Sept 2018
              ACAD_LEVEL_BOT,                                        -- Jan 2021
              decode(ACAD_LEVEL_BOT,
                     '00','Not Set',
                     '10','Freshman',
                     '20','Sophomore',
                     '30','Junior',
                     '40','Senior',
                     '50','Post-Bacc Undergraduate',
                     '60','Masters',
                     '70','Master/Doctorate',
                     '80','Doctorate',
                     '90','Certificate',
                     '100','Non-degree',
                     'Not Set')
              ACAD_LEVEL_BOT_LD,                                     -- Jan 2021
              ACAD_PROG_CD,
              ACAD_PROG_LD,
              PROG_CIP_CD,
              ACAD_PLAN_CD,
              ACAD_PLAN_LD,
              PLAN_CIP_CD,
              CE_ONLY_FLG,
              NEW_CONT_IND,
              ONLINE_HYBRID_FLG,
              ONLINE_ONLY_FLG,
              RSDNCY_ID,
              RSDNCY_LD,
              IS_RSDNCY_FLG,
              ONLINE_FTE,
              TOT_FTE,
              ONLINE_CREDITS,
              0           CE_ONLINE_CREDITS,                          -- Nov 2020
              NON_ONLINE_CREDITS,
              CE_CREDITS,
              NON_CE_CREDITS,
              TOT_CREDITS,
              ENROLL_CNT,
              ONLINE_CNT,
              CE_CNT,
              CREATED_EW_DTTM
         FROM CSMRT_OWNER.UM_M_AF_STDNT_ENRL_AMH
       UNION ALL
       SELECT INSTITUTION_CD,
              ACAD_CAR_CD,
              TERM_CD,
              PERSON_ID,
              SRC_SYS_ID,
              ACAD_YR     ACAD_YR_SID,
              AID_YEAR,
              TERM_LD,
              ACAD_ORG_CD,                                           -- Sept 2018
              ACAD_ORG_LD,                                           -- Sept 2018
              ACAD_LEVEL_BOT,                                        -- Jan 2021
              decode(ACAD_LEVEL_BOT,
                     '00','Not Set',
                     '10','Freshman',
                     '20','Sophomore',
                     '30','Junior',
                     '40','Senior',
                     'GR','Graduate',
                     'JD','Law',
                     'Not Set')
              ACAD_LEVEL_BOT_LD,                                     -- Jan 2021
              ACAD_PROG_CD,
              ACAD_PROG_LD,
              PROG_CIP_CD,
              ACAD_PLAN_CD,
              ACAD_PLAN_LD,
              PLAN_CIP_CD,
              CE_ONLY_FLG,
              NEW_CONT_IND,
              ONLINE_HYBRID_FLG,
              ONLINE_ONLY_FLG,
              RSDNCY_ID,
              RSDNCY_LD,
              IS_RSDNCY_FLG,
              ONLINE_FTE,
              TOT_FTE,
              ONLINE_CREDITS,
              CE_ONLINE_CREDITS,                                      -- Nov 2020
              NON_ONLINE_CREDITS,
              CE_CREDITS,
              NON_CE_CREDITS,
              TOT_CREDITS,
              ENROLL_CNT,
              ONLINE_CNT,
              CE_CNT,
              CREATED_EW_DTTM
         FROM CSMRT_OWNER.UM_M_AF_STDNT_ENRL_BDL;
