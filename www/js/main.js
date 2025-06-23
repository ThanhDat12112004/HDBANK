// main.js - HDBank Website
// Hiệu ứng menu active
const navLinks = document.querySelectorAll('nav ul li a');
navLinks.forEach(link => {
    link.addEventListener('click', function() {
        navLinks.forEach(l => l.parentElement.classList.remove('active'));
        this.parentElement.classList.add('active');
    });
});
// Validate form liên hệ
function validateContactForm() {
    const name = document.getElementById('name').value.trim();
    const email = document.getElementById('email').value.trim();
    const message = document.getElementById('message').value.trim();
    if (!name || !email || !message) {
        alert('Vui lòng điền đầy đủ thông tin!');
        return false;
    }
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
        alert('Email không hợp lệ!');
        return false;
    }
    alert('Gửi liên hệ thành công!');
    return true;
}
