#!/usr/bin/env bun
import { MongoClient } from "mongodb";
import fs from "fs/promises";
import { config as dotenvConfig } from "dotenv";
import { generateConfigs } from "./utils"

// Загружаем переменные окружения из файла config/.env
dotenvConfig({ path: "config/.env" });

/**
 * Чтение конфигурационного JSON-файла с настройками для поиска дубликатов.
 * Файл должен располагаться в папке config/
 */
async function loadConfigs(configFile = "output/duplicateKeys.json") {
  try {
    const data = await fs.readFile(configFile, "utf8");
    return JSON.parse(data);
  } catch (err) {
    console.error(`Ошибка чтения файла конфигураций "${configFile}":`, err);
    process.exit(1);
  }
}

/**
 * Функция поиска дубликатов по заданным полям в указанных коллекциях.
 *
 * @param {Array<{collection: string, fields: string[]}>} configs - Массив конфигураций.
 * @param {Object} options - Параметры подключения.
 * @param {string} options.uri - URI подключения к MongoDB.
 * @param {string} options.dbName - Имя базы данных.
 * @returns {Promise<Object>} - Результаты поиска дубликатов, сгруппированные по коллекциям.
 */
async function findDuplicates(configs, { uri, dbName }) {
  const client = new MongoClient(uri);
  const resultsByCollection = {};

  try {
    await client.connect();
    const db = client.db(dbName);

    // Обрабатываем каждую конфигурацию (коллекция и список полей)
    for (const config of configs) {
      const { collection: collectionName, fields } = config;
      if (!collectionName || !Array.isArray(fields)) {
        console.error("Некорректная конфигурация:", config);
        continue;
      }

      console.log(`\nОбработка коллекции: ${collectionName}`);
      const collection = db.collection(collectionName);
      const collectionResults = [];

      // Для каждого поля запускаем aggregation pipeline для поиска дубликатов
      for (const field of fields) {
        console.log(`Проверка дубликатов для поля: ${field}`);

        const pipeline = [
          {
            $group: {
              _id: `$${field}`, // Группируем по значению поля
              count: { $sum: 1 },
              docs: { $push: { _id: "$$ROOT._id" } }
            }
          },
          { $match: { count: { $gt: 1 } } },
          { $sort: { count: -1 } }
        ];

        const duplicates = await collection.aggregate(pipeline).toArray();

        if (duplicates.length > 0) {
          console.log(`Найдены дубликаты для поля "${field}" в коллекции "${collectionName}"`);
          collectionResults.push({ field, duplicates });
        } else {
          console.log(`Дубликатов по полю "${field}" в коллекции "${collectionName}" не найдено.`);
        }
      }

      if (collectionResults.length > 0) {
        resultsByCollection[collectionName] = collectionResults;
      }
    }
  } catch (error) {
    console.error("Ошибка при выполнении запроса:", error);
  } finally {
    await client.close();
  }

  return resultsByCollection;
}

(async () => {
  // Чтение параметров подключения из переменных окружения
  const uri = process.env.DB_URI || "mongodb://localhost:27017";
  const dbName = process.env.DB_NAME || "your_database_name";

  // Генерируем файл configs из updateIndex.failure.json
  await generateConfigs()

  // Загружаем конфигурацию для поиска дубликатов из файла output/duplicateKeys.json
  const configs = await loadConfigs();

  // Выполняем поиск дубликатов
  const duplicatesResults = await findDuplicates(configs, { uri, dbName });
  console.log("\nИтоговые результаты поиска дубликатов:");
  console.log(JSON.stringify(duplicatesResults, null, 2));

  // Папка для сохранения результатов
  const outputDir = "output/duplicaties_json";
  try {
    await fs.mkdir(outputDir, { recursive: true });
  } catch (err) {
    console.error(`Ошибка при создании папки "${outputDir}":`, err);
    process.exit(1);
  }

  // Записываем результаты для каждой коллекции в отдельный JSON-файл
  for (const [collectionName, results] of Object.entries(duplicatesResults)) {
    const filename = `${outputDir}/${collectionName}_duplicates.json`;
    const fileContent = JSON.stringify({ collection: collectionName, results }, null, 2);
    try {
      await fs.writeFile(filename, fileContent, "utf8");
      console.log(`Результаты для коллекции "${collectionName}" записаны в файл: ${filename}`);
    } catch (err) {
      console.error(`Ошибка записи файла для коллекции "${collectionName}":`, err);
    }
  }
})();
