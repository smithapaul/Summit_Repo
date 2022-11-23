DROP FUNCTION CSMRT_OWNER.CAR_F
/

--
-- CAR_F  (Function) 
--
CREATE OR REPLACE function CSMRT_OWNER.car_f(p_ACAD_CAR_CD varchar2) return varchar2
is
 v_dummy varchar2(50);
begin
 select decode(p_ACAD_CAR_CD,
               'ALL',   'All',
               'DOC',   'Doctorate',
               'DOC1',  'Doctorate Research Scholarship',
               'DOC2',  'Doctorate Professional',
               'GRAD',  'Graduate',
               'MED',   'Medical',
               'NON',   'Non-degree',
               'UGRD',  'Undergraduate',
               'Error') into v_dummy from DUAL;
 return v_dummy;
exception
 when others then return 'Error';
end car_f;
/
