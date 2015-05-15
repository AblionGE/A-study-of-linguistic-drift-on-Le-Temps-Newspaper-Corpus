import re

res = open('data.m', 'w')

years = []
for year in range(1840, 1995, 5):
        years.append(str(year))
res.write('years = ['+(";".join(years))+'];')
res.write("\n")
res.write("""
fig = figure;
hold on;

""")


for fileStart in ['v10-50','v20-50','v20-100']:
        for N in [6, 8]:
                fileN = fileStart+'-'+str(N)
                inp = open(fileN)
                print(fileN)

                hist = {}
                for year in range(1840, 1995, 5):
                        hist[str(year)] = 0

                lines = inp.readlines()
                for (i,line) in zip(range(1,len(lines)+1), lines):
                        year = line.split("\t")[0].split(":")[1]
                        hist[year] += 1;

                inp.close()
                vals = []
                
                for year in range(1840, 1995, 5):
                        years.append(str(year))
                        vals.append(str(hist[str(year)]))
                file = re.sub('-', '', fileN) 
                res.write(file+' = ['+(";".join(vals))+'];')
                res.write("\n")

print('done')
res.close()
