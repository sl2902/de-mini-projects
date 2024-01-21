function transform(input) {
    var obj = JSON.parse(input);
    if (obj.disease && obj.disease.match(/Cancer$/i)) {
        return JSON.stringify(obj);
    }
    return null;
}