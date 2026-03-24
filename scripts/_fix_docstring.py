import sys

f = r'c:\Users\cdroinat\OneDrive - Microsoft\1 - Microsoft\01 - Architecture\-- 004 - Demo\02 - Fabric Démo\The_AI_Skill_Analyzer\scripts\analyzer.py'

with open(f, 'r', encoding='utf-8-sig') as fh:
    content = fh.read()

# Fix double docstring: '"""\n"""The AI Skill' -> '"""The AI Skill'
old = '"""\n"""The AI Skill'
new = '"""The AI Skill'
if old in content:
    content = content.replace(old, new, 1)
    print("Fixed double docstring")
else:
    print("Double docstring not found")

# Write back without BOM
with open(f, 'w', encoding='utf-8', newline='\n') as fh:
    fh.write(content)

print("Done")
