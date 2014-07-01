package msg

const minus byte = 45 //'-'
const imaxlen = 32

var nums = [10]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}

// itoa for a writer; base 10
func writeItoa(i int64, w Writer) {
	var zbuf [imaxlen]byte
	zdx := imaxlen
	// handle 0 explicitly
	if i == 0 {
		w.WriteByte('0')
		return
	}
	// handle minus
	if i < 0 {
		w.WriteByte(minus)
		i *= -1
	}
	// write digits backwards
	for i != 0 {
		zdx--
		zbuf[zdx] = nums[i%10]
		i /= 10
	}
	// return
	w.Write(zbuf[zdx:])
	return
}

// utoa for a writer, base 10
func writeUtoa(u uint64, w Writer) {
	var zbuf [imaxlen]byte
	zdx := imaxlen
	if u == 0 {
		w.WriteByte('0')
		return
	}
	for u != 0 {
		zdx--
		zbuf[zdx] = nums[u%10]
		u /= 10
	}
	w.Write(zbuf[zdx:])
	return
}
