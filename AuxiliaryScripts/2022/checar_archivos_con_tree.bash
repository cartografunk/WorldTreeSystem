# Define la ruta base (carpeta que contiene las "madre")
$base = "C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Costa Rica\2022_ForestInventory\8-ForestMetrix_Projects"
$base = "C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Guatemala\2022_ForestInventory\4-ForestMetrix_Projects"
$base = "C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Mexico\2022_ForestInventory\4-ForestMetrix_Projects"

# Solo carpetas de primer nivel (no recursivo)
$folders = Get-ChildItem $base -Directory

$results = foreach ($folder in $folders) {
    # Todos los archivos en la carpeta
    $allFiles = Get-ChildItem $folder.FullName -File

    # Primer archivo XLSX con 'Tree' en el nombre
    $treeFile = $allFiles | Where-Object { $_.Extension -eq ".xlsx" -and $_.Name -match "Tree" } | Select-Object -First 1

    # Path del archivo 'Tree' (si existe)
    $treePath = if ($treeFile) { $treeFile.FullName } else { "NA" }

    # Total de archivos (todos los tipos)
    $totalFiles = $allFiles.Count

    # Salida como objeto
    [PSCustomObject]@{
        Carpeta = $folder.Name
        PathCompletoCarpeta = $folder.FullName
        TreeFilePath = $treePath
        TotalArchivos = $totalFiles
    }
}

# Exporta a CSV
$csvOut = "C:\Users\HeyCe\World Tree Technologies Inc\Operations - Documentos\WorldTreeSystem\AuxiliaryScripts\2022\gt_2022_cheatseet.csv"
$results | Export-Csv $csvOut -NoTypeInformation -Encoding UTF8

Write-Host "✅ Archivo exportado: $csvOut"
