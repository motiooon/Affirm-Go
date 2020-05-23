package main

import (
	. "Affirm/structures"
	"bufio"
	"encoding/csv"
	"io"
	"log"
	. "math"
	"os"
	"sort"
	"strconv"
)

// It's a factory because I need to use different struct depending on the file that I'm reading
// Tried to make it work with generic interfaces but this is what worked
func ingestionFactory(filePath string) []interface{} {

	csvFile, _ 		:= os.Open(filePath)
	reader 			:= csv.NewReader(bufio.NewReader(csvFile))

	var result []interface {}
	var i = 0

	for {
		line, error := reader.Read()

		if i == 0 {
			// skip th first csv line, a little byzantine but should work
			i++
			continue
		}

		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		switch filePath {

		case FACILITIES_PATH:
			// we should handle parse errors on these in case csv is corrupted
			amount, _ 		:= strconv.ParseFloat(line[0], 64)
			interestRate, _ := strconv.ParseFloat(line[1], 64)
			id, _ 			:= strconv.Atoi(line[2])
			bankId, _ 		:= strconv.Atoi(line[3])

			// we need a slice of pointers here because we would need to operate on the amount in the code
			// and we dont want a copy we want to be able to change the value as we grant loans
			result = append(result, &Facility{
				Amount:     	amount,
				InterestRate: 	interestRate,
				Id:           	id,
				BankId:       	bankId,
			})

		case COVENANTS_PATH:
			// we should handle parse errors on these in case csv is corrupted
			facilityId, _ 			:= strconv.Atoi(line[0])
			maxDefaultLikelihood, _ := strconv.ParseFloat(line[1], 64)
			bankId, _ 				:= strconv.Atoi(line[2])
			bannedState				:= line[3]

			result = append(result, Covenant{
				FacilityId:           facilityId,
				MaxDefaultLikelihood: maxDefaultLikelihood,
				BankId:               bankId,
				BannedState:          bannedState,
			})

		case LOANS_PATH:

			// we should handle parse errors on these in case csv is corrupted
			interestRate, _ 		:= strconv.ParseFloat(line[0], 64)
			amount, _ 				:= strconv.ParseFloat(line[1] ,64)
			id, _ 					:= strconv.Atoi(line[2])
			defaultLikelihood, _ 	:= strconv.ParseFloat(line[3], 64)
			state 					:= line[4]

			result = append(result, Loan{
				InterestRate:     	interestRate,
				Amount: 			amount,
				Id:           		id,
				DefaultLikelihood:  defaultLikelihood,
				State: 				state,
			})
		}
	}

	return result
}

func ingestData() ([]interface{}, []interface{}, []interface{}){
	facilities 	:= ingestionFactory(FACILITIES_PATH)
	covenants 	:= ingestionFactory(COVENANTS_PATH)
	loans 		:= ingestionFactory(LOANS_PATH)

	return facilities, covenants, loans
}

// Calculates Yield
func calculateYield (loan Loan, facility *Facility) float64 {
	return 	(1 - loan.DefaultLikelihood) * (loan.InterestRate * loan.Amount) -
			(loan.DefaultLikelihood * loan.Amount) - (facility.InterestRate * loan.Amount)
}

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

// Declaring the two new types that we need to work with and then write to output files
type assignment struct {
	loanId     int
	facilityId int
}

type yield struct {
	facilityId    int
	expectedYield float64
}

// Helper function to write to a CSV file
func writeCSV(filePath string, data [][]string) {

	csvFile, err := os.Create(filePath)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	csvWriter := csv.NewWriter(csvFile)

	for _, row := range data {
		_ = csvWriter.Write(row)
	}

	csvWriter.Flush()
	csvFile.Close()
}

func main(){
	// ingest data
	facilities , covenants, loans := ingestData()

	type coven struct {
		bannedStates []string
		maxDefaultLikelihood float64
	}

	// make a O(1) map facilities:covenants
	facCov := make(map[int]coven)

	for i := 0; i < len(covenants); i++ {

		cov, _ := covenants[i].(Covenant)

		// this is a tricky one in golang, I need to check a key, rather than the literal object 0 values
		if facCov[cov.FacilityId].bannedStates != nil {

			facCov[cov.FacilityId] = coven{
				append(facCov[cov.FacilityId].bannedStates, cov.BannedState),
				Max(cov.MaxDefaultLikelihood, facCov[cov.FacilityId].maxDefaultLikelihood),
			}

		} else {
			bs := make([]string, 1)
			bs = append(bs, cov.BannedState)

			facCov[cov.FacilityId] = coven{
				bs[1:],
				cov.MaxDefaultLikelihood,
			}
		}
		
	}

	// Will use these 2 for the output data. Start with length of 1 but will push things in here
	loansToFacilities := make([]assignment, 0)
	facilitiesYield := make(map[int]float64)

	for i := 0; i < len(loans); i++ {

		var (
			maxYield float64
			electedFacility *Facility
			winningYield float64
		)

		// Had to type cast it to Loan as I've used interface so I can ingest all types
		loan, _ := loans[i].(Loan)

		for f := 0; f < len(facilities); f++ {

			// Had to type cast it to *Facility as I've used generic interface so I can ingest all types
			// We need the pointer as we will change the value if the facility has been elected to grant the loan
			// ‘Type assertion’ in Go language
			facility, _ := facilities[f].(*Facility)

			if
				// Facility has money left
				facility.Amount >= loan.Amount &&

				// The state of the loan is not banned by covenant
				!Contains(facCov[facility.Id].bannedStates, loan.State) &&

				// Max default likelihood of the loan is is within range for the fac:cov
				facCov[facility.Id].maxDefaultLikelihood >= loan.DefaultLikelihood {

					currentYield := calculateYield(loan, facility)
					maxYield = Max(currentYield, maxYield)

					if maxYield == currentYield {
						electedFacility = facility
						winningYield = currentYield
					}

				}
		}

		// if the maxYield is still 0 means we did not find a facility, otherwise we did and we store that
		if maxYield != 0 && electedFacility != nil{

			// Push loan assignment
			loansToFacilities = append(loansToFacilities, assignment{
				loan.Id,
				electedFacility.Id,
			})

			// subtract loan amount from the total facility
			electedFacility.Amount -= loan.Amount

			// attribute the winning Yield to the facility
			if val, ok := facilitiesYield[electedFacility.Id];  ok{
				facilitiesYield[electedFacility.Id] = val + winningYield
			} else {
				facilitiesYield[electedFacility.Id] = winningYield
			}
		}
	}

	facilitiesYieldList := make([][]string, 0)
	facilitiesYieldList = append(facilitiesYieldList, []string{
		"facility_id",
		"expected_yield",
	})

	keys := make([]int, 0, len(facilitiesYield))
	for k := range facilitiesYield {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// make the facilitiesYield into an array of strings so I can make it into the csv
	for _, k := range keys {
		facilitiesYieldList = append(facilitiesYieldList, []string{
			strconv.FormatInt(int64(k), 10),
			strconv.FormatFloat(Round(facilitiesYield[k] * 100)/100, 'f', -1, 64),
		})
	}

	// turn loansToFacilities into an [][]string so we can push it into the csv
	loansToFacilitiesList := make([][]string, 0)
	loansToFacilitiesList = append(loansToFacilitiesList, []string{
		"loan_id",
		"facility_id",
	})

	for _, value:= range loansToFacilities {
		loansToFacilitiesList = append(loansToFacilitiesList, []string{
			strconv.FormatInt(int64(value.loanId), 10),
			strconv.FormatInt(int64(value.facilityId), 10),
		})
	}

	// Write the two CSV files
	writeCSV("yields.csv", facilitiesYieldList)
	writeCSV("assignments.csv", loansToFacilitiesList)
}