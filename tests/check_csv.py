import csv,os,subprocess,tempfile,pathlib,sys
root=pathlib.Path(__file__).resolve().parents[1]/'fixtures'
bin=pathlib.Path(__file__).resolve().parents[1]/'velocity'
ok=0
for case in sorted(root.iterdir()):
 if not case.is_dir():continue
 inp=case/'input'
 if not inp.exists():inp=case/'data'
 with tempfile.TemporaryDirectory() as o:
  r=subprocess.run([str(bin),str(inp),o],capture_output=True,text=True,env={k:v for k,v in os.environ.items() if k not in ('DATA_DIR','OUTPUT_DIR')})
  if r.returncode:print('ERROR',case.name,r.stderr);continue
  matches=True
  for tbl in ('user_daily','campaign_daily'):
   with open(case/'expected'/f'{tbl}.csv',newline='') as f:expected=list(csv.DictReader(f))
   with open(pathlib.Path(o)/tbl/'result.csv',newline='') as f:actual=list(csv.DictReader(f))
   a=sorted(tuple(sorted(x.items())) for x in actual)
   e=sorted(tuple(sorted(x.items())) for x in expected)
   if a!=e:
    print('FAIL',case.name,tbl,'EXPECTED',expected,'GOT',actual)
    matches=False
  if matches:ok+=1;print('ok',case.name)
print(f'{ok} passed')
sys.exit(0 if ok==len([x for x in root.iterdir() if x.is_dir()]) else 1)
