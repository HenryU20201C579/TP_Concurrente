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

type Record struct {
	Titular       string
	Departamento  string
	Provincia     string
	Distrito      string
	Ubigeo        string
	FechaAlta     string
	Tarifa        string
	Periodo       string
	ConsumoKwatts float64
	Facturacion   float64
	StatusCliente string
	FechaCorte    string
}

func main() {
	// Abre el archivo CSV
	file, err := os.Open("TP-Concurrente.csv")
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
		data := Record{
			Titular:       record[0],
			Departamento:  record[1],
			Provincia:     record[2],
			Distrito:      record[3],
			Ubigeo:        record[4],
			FechaAlta:     record[5],
			Tarifa:        record[6],
			Periodo:       record[7],
			ConsumoKwatts: parseFloat(record[8]),
			Facturacion:   parseFloat(record[9]),
			StatusCliente: record[10],
			FechaCorte:    record[11],
		}

		// Realiza operaciones de Map (por ejemplo, calcular el consumo promedio por departamento)
		// En este ejemplo, simplemente sumamos el consumo de kWatt por departamento
		mu.Lock()
		result[data.Departamento] += data.ConsumoKwatts
		mu.Unlock()
	}

	// Función Reduce
	reduceFunc := func() {
		defer wg.Done()

		// Realiza operaciones de Reduce (por ejemplo, imprimir el resultado)
		for department, totalConsumption := range result {
			fmt.Printf("Departamento: %s, Consumo Total (kWatt): %.2f\n", department, totalConsumption)
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

func parseFloat(value string) float64 {
	value = strings.ReplaceAll(value, ",", "")
	result, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Printf("Error al analizar el valor: %v\n", err)
		return 0.0
	}
	return result
}
