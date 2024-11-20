import fs from "node:fs";
import csv from "csv-parser";
import Joi from "joi";
import dotenv from "dotenv";

dotenv.config();

const uuidSchema = Joi.string().uuid().required();
const driversCsvData = [];
const hubs = [
    { name: "mumbai", id: process.env.MUMBAI_ID },
    { name: "hyderabad", id: process.env.HYDERBAD_ID },
    { name: "delhi", id: process.env.DELHI_ID },
    { name: "banglore", id: process.env.BANGALORE_ID }
]
const uberDrivers = [];

const fetchUberDrivers = async (orgId, pgNo) => {
    const myHeaders = new Headers();
    myHeaders.append("Authorization", `Bearer ${process.env.UBER_TOKEN}`);
    const requestOptions = {
        method: "GET",
        headers: myHeaders,
        redirect: "follow"
    };
    try {
        const response = await fetch(`https://api.uber.com/v1/vehicle-suppliers/drivers?page_token=${pgNo}&page_size=${process.env.API_PAGE_SIZE}&org_id=${orgId}&include_assigned_vehicles=false`, requestOptions);
        const json = await response.json();
        return json;
    } catch (err) {
        console.log(err);
    }

}


const fetchAllHubDrivers = async () => {
    for (const hub of hubs) {
        const orgId = hub.id;
        let pgNo = 1;
        console.log(`hub ${hub.name}: ${orgId}`);
        while (pgNo) {
            console.log("page no: " + pgNo);
            const jsonData = await fetchUberDrivers(orgId, pgNo);
            const { paginationResult, driverInformation } = jsonData;

            const { nextPageToken } = paginationResult;
            if (pgNo === nextPageToken) {
                console.log(`same page token ${pgNo}`);
                break;
            }
            pgNo = nextPageToken;

            for (const driver of driverInformation) {
                uberDrivers.push(driver);
            }
        }
    }
}

const finalData = [];

fetchAllHubDrivers()
    .then(() => {
        console.log('uberDriver');
        console.log(uberDrivers[0]);
        console.log(`uber drivers: ${uberDrivers.length}`);

        fs.writeFile("uberDrivers.json", JSON.stringify(uberDrivers), (err) => {
            if (err) {
                console.log(err);
            }
        });

        fs.createReadStream('driver-sheet.csv')
            .pipe(csv())
            .on('data', (data) => driversCsvData.push(data))
            .on('end', () => {
                let foundDriver = 0;
                for (let i = 0; i < driversCsvData.length; i++) {
                    const driverCSV = driversCsvData[i];
                    const driverUberId = driverCSV['driver_uber_id']

                    const { error, value } = uuidSchema.validate(driverUberId);
                    let uberFirstName = "", uberLastName = "", uberPhoneNumber = "", incorrectUberIdReason = "";
                    if (error) {
                        if (error.message.includes("is not allowed to be empty")) {
                            incorrectUberIdReason = "Uber id is empty";
                            finalData.push({ ...driverCSV, uberFirstName, uberLastName, uberPhoneNumber, incorrectUberIdReason });
                        } else {
                            incorrectUberIdReason = error.message;
                            finalData.push({ ...driverCSV, uberFirstName, uberLastName, uberPhoneNumber, incorrectUberIdReason });
                        }
                        continue;
                    }
                    const driver = uberDrivers.find(driver => {
                        return driver.driverId === driverUberId
                    });
                    let isBreakFlag = false;
                    if (driver) {
                        foundDriver++;
                        uberFirstName = driver.firstName;
                        uberLastName = driver.lastName;
                        uberPhoneNumber = driver.phoneNumber.number;
                        // isBreakFlag = true;
                    } else {
                        incorrectUberIdReason = "Uber driver not found with this uber id";
                    }
                    const newData2Save = {
                        ...driverCSV,
                        uberFirstName,
                        uberLastName,
                        uberPhoneNumber,
                        incorrectUberIdReason
                    }
                    finalData.push(newData2Save);
                    if (isBreakFlag) break;

                }
                console.log('total sheet drivers: ' + driversCsvData.length);
                console.log('found drivers: ' + foundDriver);
                console.log("uber drivers: " + uberDrivers.length);
                console.log('final data: ' + finalData.length);

                fs.writeFile("finalData.json", JSON.stringify(finalData), (err) => {
                    if (err) {
                        console.log(err);
                        return;
                    }

                    console.log("final data written to file: finalData.json");
                })


            });


    });