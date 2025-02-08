import fs from "fs/promises";
import path from "path";

/**
 * Генерирует новый конфигурационный файл на основе входного файла с ошибками.
 * Входной файл (updateIndex.failure.json) должен содержать структуру, где ключами являются имена коллекций,
 * а значениями – объекты, в которых ключи (например, "email_1") представляют имена индексов.
 *
 * Если для конкретного индекса обнаружена ошибка DuplicateKey (код 11000),
 * то имя индекса (без суффикса "_1") добавляется в список полей для этой коллекции.
 *
 * Результат записывается в виде массива объектов:
 * [
 *   {
 *     "collection": "имя коллекции",
 *     "fields": ["поле1", "поле2", ...]
 *   },
 *   ...
 * ]
 */
export async function generateConfigs() {
  try {
    const inputFile = path.resolve('input', 'updateIndex.failure.json')  
    const outputFile = path.resolve('output', 'duplicates_json', 'duplicateKeys.json')  

    const content = await fs.readFile(inputFile, "utf8");
    const data = JSON.parse(content);

    const newConfigs = [];

    // Перебираем коллекции
    for (const collectionName in data) {
      if (!Object.prototype.hasOwnProperty.call(data, collectionName)) continue;

      const indexes = data[collectionName];
      const fields = [];

      // Перебираем ключи (имена индексов) внутри коллекции
      for (const indexName in indexes) {
        if (!Object.prototype.hasOwnProperty.call(indexes, indexName)) continue;

        const indexData = indexes[indexName];
        // Проверяем, что ошибка является DuplicateKey.
        // Может быть, код ошибки находится в indexData.err.code или в indexData.err.errorResponse.code
        const errCode = indexData.err.code || (indexData.err.errorResponse && indexData.err.errorResponse.code);
        if (errCode === 11000) {
          // Удаляем суффикс "_1" из имени поля, если он есть
          const fieldName = indexName.replace(/_1$/, "");
          fields.push(fieldName);
        }
      }

      // Если для коллекции найдены поля с ошибкой DuplicateKey, добавляем запись в результирующий массив
      if (fields.length > 0) {
        newConfigs.push({
          collection: collectionName,
          fields: fields
        });
      }
    }

    // Записываем новый конфигурационный файл с отступами в 2 пробела
    await fs.writeFile(outputFile, JSON.stringify(newConfigs, null, 2), "utf8");
    console.log(`Новый файл конфигурации успешно сформирован: ${outputFile}`);
  } catch (err) {
    console.error("Ошибка при генерации файла конфигурации:", err);
  }
}



