import ast
import os
import subprocess
from glob import glob

project_root = os.path.abspath("D:/LOCAL/WorldTreeSystem")
cruises_path = os.path.join(project_root, "CruisesProcessor")
output_dot = os.path.join(project_root, "cruises_ast_map.dot")
output_png = os.path.join(project_root, "cruises_ast_map.png")

functions = set()
calls = set()
edges = set()

# 1. Parseo de funciones y llamadas
for filepath in glob(os.path.join(cruises_path, "**", "*.py"), recursive=True):
    with open(filepath, "r", encoding="utf-8") as file:
        try:
            tree = ast.parse(file.read(), filename=filepath)
        except Exception as e:
            print(f"❌ Error parsing {filepath}: {e}")
            continue

        current_func = None
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_name = f"{os.path.basename(filepath)}::{node.name}"
                functions.add(func_name)
                current_func = func_name
            elif isinstance(node, ast.Call):
                if current_func:
                    if isinstance(node.func, ast.Name):
                        callee = node.func.id
                        calls.add(callee)
                        edges.add((current_func, callee))
                    elif isinstance(node.func, ast.Attribute):
                        callee = node.func.attr
                        calls.add(callee)
                        edges.add((current_func, callee))

# 2. Escribir archivo DOT
with open(output_dot, "w", encoding="utf-8") as f:
    f.write("digraph G {\n")
    for fn in sorted(functions):
        f.write(f'    "{fn}";\n')
    for a, b in sorted(edges):
        f.write(f'    "{a}" -> "{b}";\n')
    f.write("}\n")
print(f"✅ DOT generado: {output_dot}")

# 3. Intentar convertir a PNG automáticamente
try:
    subprocess.run(["dot", "-Tpng", output_dot, "-o", output_png], check=True)
    print(f"🖼 Imagen generada: {output_png}")
except FileNotFoundError:
    print("⚠️ No se encontró Graphviz (dot.exe). Instálalo o verifica tu PATH.")
except Exception as e:
    print(f"❌ Error al generar imagen: {e}")

# 4. (Opcional) Mostrar funciones nunca llamadas
unused = [f for f in functions if f.split("::")[1] not in calls]
if unused:
    print("\n🔍 Funciones definidas pero no llamadas por nadie:")
    for fn in sorted(unused):
        print(f"  - {fn}")
else:
    print("\n✅ Todas las funciones tienen al menos una llamada")
