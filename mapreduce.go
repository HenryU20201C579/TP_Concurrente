package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Project struct {
	Date         string
	ProjectName  string
	Department   string
	Province     string
	District     string
	TotalCost    float64
	Stage        string
	PhysicalProg float64
	Ubigeo       string
	Contractor   string
}

func main() {
	// Abre el archivo CSV
	file, err := os.Open("Dataset-proyectos.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Lee el contenido del archivo CSV y omite la primera fila (encabezados)
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	records = records[1:] // Omitir la primera fila (encabezados)

	// Define las funciones Map y Reduce
	var wg sync.WaitGroup
	var mu sync.Mutex
	result := make(map[string]float64)

	// Función Map
	mapFunc := func(record []string) {
		defer wg.Done()

		// Procesa cada registro del dataset
		project := Project{
			Date:         record[0],
			ProjectName:  record[1],
			Department:   record[2],
			Province:     record[3],
			District:     record[4],
			TotalCost:    parseCost(record[5]),
			Stage:        record[6],
			PhysicalProg: parseProgress(record[7]),
			Ubigeo:       record[8],
			Contractor:   record[9],
		}

		// Realiza operaciones de Map (por ejemplo, calcular el costo promedio por departamento)
		// En este ejemplo, simplemente sumamos el costo total por departamento
		mu.Lock()
		result[project.Department] += project.TotalCost
		mu.Unlock()
	}

	// Función Reduce
	reduceFunc := func() {
		defer wg.Done()

		// Realiza operaciones de Reduce (por ejemplo, imprimir el resultado)
		for department, totalCost := range result {
			fmt.Printf("Departamento: %s, Costo Total: %.2f\n", department, totalCost)
		}
	}

	// Ejecuta las goroutines Map
	wg.Add(len(records))
	for _, record := range records {
		go mapFunc(record)
	}

	// Ejecuta la goroutine Reduce
	wg.Add(1)
	go reduceFunc()

	// Espera a que todas las goroutines terminen
	wg.Wait()
}

func parseCost(costStr string) float64 {
	costStr = strings.ReplaceAll(costStr, "$", "")
	costStr = strings.ReplaceAll(costStr, ",", "")
	cost, err := strconv.ParseFloat(costStr, 64)
	if err != nil {
		log.Printf("Error al analizar el costo: %v\n", err)
		return 0.0
	}
	return cost
}

func parseProgress(progressStr string) float64 {
	progressStr = strings.ReplaceAll(progressStr, "%", "")
	progress, err := strconv.ParseFloat(progressStr, 64)
	if err != nil {
		log.Printf("Error al analizar el avance físico: %v\n", err)
		return 0.0
	}
	return progress
}
